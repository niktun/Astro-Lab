# dags/se_email_elt_daily.py
"""
Daily ELT pipeline that reads sample emails from a bundled JSON file,
normalizes them, and upserts into Postgres (Neon) using an Airflow
connection called MY_POSTGRES.

Why it exists:
- Show a simple, production-shaped ELT flow (extract → transform → load → DQ).
- Make it portable to Hosted by bundling the sample JSON under dags/assets.
- Keep XCom values safe (strings instead of datetime objects).
"""

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

# Basic retry behavior that’s reasonable for demos
DEFAULTS = {"retries": 2, "retry_delay": timedelta(minutes=5)}


def _normalize_keys(d: Dict[str, Any]) -> Dict[str, Any]:
    """Lower-case keys and swap hyphens for underscores so we can address fields consistently."""
    return {(k or "").lower().replace("-", "_"): v for k, v in d.items()}


def _deterministic_id(parts: List[str]) -> str:
    """Build a stable message_id when an email doesn’t have one."""
    basis = "|".join(p or "" for p in parts)
    return "gen-" + hashlib.sha256(basis.encode("utf-8")).hexdigest()[:24]


def _to_datetime(value):
    """
    Try to convert common email date formats to a timezone-aware datetime.
    We accept either RFC-2822 (typical email Date header) or ISO-8601.
    """
    if not value:
        return None
    # Email-style date (e.g., "Fri, 19 Oct 2001 14:17:12 -0700 (PDT)")
    try:
        dt = parsedate_to_datetime(str(value))
        if dt and dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        pass
    # ISO-8601 fallback, including trailing 'Z'
    try:
        s = str(value).replace("Z", "+00:00")
        return datetime.fromisoformat(s)
    except Exception:
        return None


@dag(
    dag_id="se_email_elt_daily",
    start_date=datetime(2025, 1, 1),
    schedule="0 8 * * *",  # run once every morning
    catchup=False,
    default_args=DEFAULTS,
    dagrun_timeout=timedelta(minutes=20),
    tags=["se-lab", "serial", "emails"],
)
def se_email_elt_daily():
    @task
    def extract() -> List[Dict[str, Any]]:
        """
        Read sample emails from disk.

        Hosted runs package DAG code into a bundle, so the safe place to store
        demo data is under dags/assets. We also keep a couple of fallbacks so
        the same code works in local dev without changes.

        Supports:
        - JSON array
        - Single JSON object
        - NDJSON (one JSON object per line)
        """
        base = Path(__file__).resolve().parent
        candidates = [
            base / "assets" / "emails.json",              # Hosted bundle path
            base / "emails.json",                         # optional: next to the DAG
            base.parents[1] / "include" / "emails.json",  # legacy local path
        ]
        json_path = next((p for p in candidates if p.exists()), None)
        if not json_path:
            raise FileNotFoundError(
                "emails.json not found. Put it at dags/assets/emails.json."
            )

        text = json_path.read_text(encoding="utf-8")

        # Try full JSON first; if that fails, assume NDJSON.
        try:
            data = json.loads(text)
            if isinstance(data, dict):
                data = [data]
        except json.JSONDecodeError:
            data = [json.loads(line) for line in text.splitlines() if line.strip()]

        logging.info("extract: loaded %d records from %s", len(data), json_path)
        return data

    @task(queue="cpuheavy")
    def normalize(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Clean up each record so it's easy to load into SQL:
        - Harmonize keys (lowercase + underscores).
        - Convert recipients to a comma-separated string.
        - Parse the sent timestamp and emit as an ISO string (XCom-friendly).
        - Ensure we always have a message_id (generate one if missing).
        """
        out: List[Dict[str, Any]] = []
        dropped = 0

        for r in rows:
            rr = _normalize_keys(r)

            msg_id = rr.get("message_id")
            sent_at_raw = rr.get("date") or rr.get("sent_at")
            sent_at_dt = _to_datetime(sent_at_raw)
            # XCom serializes best when we pass strings, not datetime objects
            sent_at = sent_at_dt.isoformat() if sent_at_dt else None

            sender = rr.get("from")
            recipients = rr.get("to") or rr.get("recipients")
            subject = rr.get("subject")
            body = rr.get("body") or rr.get("text") or ""

            # Recipients can be a list or a string; normalize to a single string
            if isinstance(recipients, list):
                recipients_str = ",".join([str(x) for x in recipients if x])
            elif isinstance(recipients, str):
                recipients_str = recipients
            else:
                recipients_str = ""

            # Create a stable ID when the source didn't give us one
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
                    "sent_at": sent_at,  # ISO string (or None)
                    "sender": sender,
                    "recipients": recipients_str,
                    "subject": subject,
                    "body_len": len(body or ""),
                }
            )

        logging.info("normalize: %d records (dropped %d)", len(out), dropped)
        if out:
            sample = out[0].copy()
            logging.info("normalize sample: %s", json.dumps(sample, ensure_ascii=False)[:500])
        return out

    @task
    def load_to_postgres(rows: List[Dict[str, Any]]) -> int:
        """
        Create the destination table if needed and upsert the rows.
        Uses the Airflow connection `MY_POSTGRES`, which you set on the
        Deployment as `AIRFLOW_CONN_MY_POSTGRES`.
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

        hook.run(create_sql)

        inserted = 0
        with hook.get_conn() as conn, conn.cursor() as cur:
            for rec in rows:
                cur.execute(upsert_sql, rec)
                inserted += 1

        logging.info("load_to_postgres: upserted %d rows", inserted)
        return inserted

    @task
    def dq_check(inserted: int):
        """
        Basic “did we load anything?” guard.
        This fails the run loudly if nothing landed in the table.
        """
        assert inserted > 0, "DQ failure: 0 rows inserted into public.emails"

    # Wire the steps together in the obvious order
    dq_check(load_to_postgres(normalize(extract())))


dag = se_email_elt_daily()
