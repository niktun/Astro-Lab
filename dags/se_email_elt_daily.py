# dags/se_email_elt_daily.py
# End-to-end email JSON → Postgres ELT DAG (Airflow 3.x compatible)
# - Robust key normalization (handles 'Message-ID', 'From', etc.)
# - RFC-2822/ISO date parsing → tz-aware datetime, returned as ISO strings for XCom safety
# - Deterministic message_id if missing
# - TIMESTAMPTZ table + idempotent upsert
# - Optional queue routing for normalize() (e.g., to 'cpuheavy' in Astro)

from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Dict, List

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook


DEFAULTS = {"retries": 2, "retry_delay": timedelta(minutes=5)}


def _normalize_keys(d: Dict[str, Any]) -> Dict[str, Any]:
    """Lowercase keys and replace hyphens with underscores."""
    return {(k or "").lower().replace("-", "_"): v for k, v in d.items()}


def _deterministic_id(parts: List[str]) -> str:
    """Build a stable ID when message_id is missing."""
    basis = "|".join(p or "" for p in parts)
    return "gen-" + hashlib.sha256(basis.encode("utf-8")).hexdigest()[:24]


def _to_datetime(value):
    """Parse many email/ISO date formats into tz-aware datetime (or None)."""
    if not value:
        return None
    # Try RFC-2822 / email Date header
    try:
        dt = parsedate_to_datetime(str(value))
        if dt and dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        pass
    # Try ISO-8601 (support trailing Z)
    try:
        s = str(value).replace("Z", "+00:00")
        return datetime.fromisoformat(s)
    except Exception:
        return None


@dag(
    dag_id="se_email_elt_daily",
    start_date=datetime(2025, 1, 1),
    schedule="0 8 * * *",  # 8:00 AM daily
    catchup=False,
    default_args=DEFAULTS,
    dagrun_timeout=timedelta(minutes=20),
    tags=["se-lab", "serial", "scheduled", "emails"],
)
def se_email_elt_daily():
@task
def extract() -> List[Dict[str, Any]]:
    """
    Load records from a JSON file. On Hosted, read from dags/assets/emails.json.
    Fallbacks keep local dev working if you still have include/emails.json.
    Supports:
      - JSON array
      - Single JSON object
      - NDJSON (one JSON object per line) as fallback
    """
    base = Path(__file__).resolve().parent
    candidates = [
        base / "assets" / "emails.json",                # Hosted bundle location
        base / "emails.json",                           # optional: next to the DAG
        base.parents[1] / "include" / "emails.json",    # legacy local path
    ]
    json_path = next((p for p in candidates if p.exists()), None)
    if not json_path:
        raise FileNotFoundError(
            "emails.json not found. Put it at dags/assets/emails.json (Hosted bundles files under dags/)."
        )

    text = json_path.read_text(encoding="utf-8")

    # Try normal JSON first (array or single object)
    try:
        data = json.loads(text)
        if isinstance(data, dict):
            data = [data]
    except json.JSONDecodeError:
        # Fallback: NDJSON (one JSON object per line)
        data = [json.loads(line) for line in text.splitlines() if line.strip()]

    logging.info("extract: loaded %d raw records from %s", len(data), json_path)
    return data

@task(queue="cpuheavy")  # Route to a separate worker queue if configured
    def normalize(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Normalize keys/fields:
          - keys → lowercase + underscores (Message-ID → message_id)
          - recipients → comma-separated string
          - sent_at → ISO string from tz-aware datetime (RFC-2822/ISO)
          - message_id → required; generate deterministic one if missing
        """
        out: List[Dict[str, Any]] = []
        dropped = 0

        for r in rows:
            rr = _normalize_keys(r)

            msg_id = rr.get("message_id")
            sent_at_raw = rr.get("date") or rr.get("sent_at")
            sent_at_dt = _to_datetime(sent_at_raw)            # datetime or None
            sent_at = sent_at_dt.isoformat() if sent_at_dt else None  # ISO string for XCom safety

            sender = rr.get("from")
            recipients = rr.get("to") or rr.get("recipients")
            subject = rr.get("subject")
            body = rr.get("body") or rr.get("text") or ""

            # Normalize recipients → string
            if isinstance(recipients, list):
                recipients_str = ",".join([str(x) for x in recipients if x])
            elif isinstance(recipients, str):
                recipients_str = recipients
            else:
                recipients_str = ""

            # Make a stable ID if missing
            if not msg_id:
                msg_id = _deterministic_id(
                    [str(sender), recipients_str, str(subject), str(sent_at_raw), body]
                )

            if not msg_id:
                dropped += 1
                continue

            out.append(
                {
                    "message_id": msg_id,
                    "sent_at": sent_at,   # ISO string (or None)
                    "sender": sender,
                    "recipients": recipients_str,
                    "subject": subject,
                    "body_len": len(body or ""),
                }
            )

        logging.info("normalize: produced %d records (dropped %d)", len(out), dropped)
        if out:
            sample = out[0].copy()
            logging.info("normalize sample: %s", json.dumps(sample, ensure_ascii=False)[:500])
        return out

    @task
    def load_to_postgres(rows: List[Dict[str, Any]]) -> int:
        """
        Create emails table (if needed) and upsert rows into Postgres via MY_POSTGRES connection.
        """
        hook = PostgresHook(postgres_conn_id="MY_POSTGRES")

        create_sql = """
        CREATE TABLE IF NOT EXISTS public.emails (
          message_id TEXT PRIMARY KEY,
          sent_at    TIMESTAMPTZ,
          sender     TEXT,
          recipients TEXT,
          subject    TEXT,
          body_len   INT,
          loaded_at  TIMESTAMPTZ DEFAULT NOW()
        );
        """

        upsert_sql = """
        INSERT INTO public.emails (message_id, sent_at, sender, recipients, subject, body_len)
        VALUES (%(message_id)s, %(sent_at)s, %(sender)s, %(recipients)s, %(subject)s, %(body_len)s)
        ON CONFLICT (message_id) DO UPDATE SET
          sent_at=EXCLUDED.sent_at,
          sender=EXCLUDED.sender,
          recipients=EXCLUDED.recipients,
          subject=EXCLUDED.subject,
          body_len=EXCLUDED.body_len;
        """

        # Ensure table exists
        hook.run(create_sql)

        inserted = 0
        with hook.get_conn() as conn, conn.cursor() as cur:
            for rec in rows:
                cur.execute(upsert_sql, rec)
                inserted += 1

        logging.info("load_to_postgres: upserted %d rows", inserted)
        return inserted

    @task()
    def dq_check(inserted: int):
        assert inserted > 0, "DQ failure: 0 rows inserted into public.emails"

    dq_check(load_to_postgres(normalize(extract())))


dag = se_email_elt_daily()
