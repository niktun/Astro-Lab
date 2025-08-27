# dags/se_email_parallel_enrichment.py (Hosted-friendly)
# Parallel enrichment demo over a static email JSON file.
# Four independent tasks run in parallel; each returns a tiny summary.
# Airflow 3.x compatible (no SLA param, XCom-safe values) and uses
# a bundled data file path under dags/assets to work with DAG Bundles.

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List
from datetime import datetime

from airflow.decorators import dag, task


def _load_emails() -> List[Dict[str, Any]]:
    """Load emails from a bundled JSON file.

    Search order (first existing wins):
      1) dags/assets/emails.json           # Hosted bundle location
      2) dags/emails.json                  # optional: next to this DAG
      3) include/emails.json               # legacy local path for dev

    Supports:
      - JSON array
      - Single JSON object
      - NDJSON (one JSON object per line)
    """
    base = Path(__file__).resolve().parent
    candidates = [
        base / "assets" / "emails.json",
        base / "emails.json",
        base.parents[1] / "include" / "emails.json",
    ]
    p = next((c for c in candidates if c.exists()), None)
    if not p:
        raise FileNotFoundError(
            "emails.json not found. Put it at dags/assets/emails.json (Hosted bundles files under dags/)."
        )

    text = p.read_text(encoding="utf-8")
    try:
        data = json.loads(text)
        if isinstance(data, dict):
            data = [data]
    except json.JSONDecodeError:
        data = [json.loads(line) for line in text.splitlines() if line.strip()]
    return data


@dag(
    dag_id="se_email_parallel_enrichment",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["se-lab", "parallel", "emails"],
)
def se_email_parallel_enrichment():
    @task
    def sentiment_summary() -> Dict[str, Any]:
        rows = _load_emails()
        # toy sentiment signal: average body length threshold
        body_lens = [len((r.get("body") or r.get("text") or "")) for r in rows]
        avg_len = (sum(body_lens) / len(body_lens)) if body_lens else 0
        sentiment = "pos" if avg_len > 80 else ("neg" if avg_len < 20 else "neutral")
        return {"avg_body_len": avg_len, "sentiment": sentiment}

    @task
    def spam_score() -> Dict[str, Any]:
        rows = _load_emails()
        spammy = sum(1 for r in rows if "[SPAM]" in (r.get("subject") or ""))
        return {"spam_count": spammy, "total": len(rows)}

    @task
    def entity_extract() -> Dict[str, Any]:
        rows = _load_emails()
        # toy entity count: number of '@' tokens across bodies
        entities = sum((r.get("body") or r.get("text") or "").count("@") for r in rows)
        return {"entity_mentions": entities}

    @task
    def thread_stats() -> Dict[str, Any]:
        rows = _load_emails()
        subjects: Dict[str, int] = {}
        for r in rows:
            s = r.get("subject") or ""
            subjects[s] = subjects.get(s, 0) + 1
        max_thread = max(subjects.values()) if subjects else 0
        return {"max_thread_length": max_thread, "unique_subjects": len(subjects)}

    # Parallel fan-out (no dependencies between tasks)
    sentiment_summary()
    spam_score()
    entity_extract()
    thread_stats()


dag = se_email_parallel_enrichment()
