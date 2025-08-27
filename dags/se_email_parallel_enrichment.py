# dags/se_email_parallel_enrichment.py
"""
Parallel “read-only” analytics over the same sample email data.
This DAG doesn't write to the database. It runs a few small calculations
in parallel to show concurrency and independent tasks.

What you’ll see:
- Four tasks that don’t depend on each other.
- Each task loads the same bundled emails JSON and returns a tiny summary.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List
from datetime import datetime

from airflow.decorators import dag, task


def _load_emails() -> List[Dict[str, Any]]:
    """
    Load the bundled sample data from disk.

    Hosted bundles files under dags/, so the primary path is dags/assets/emails.json.
    We keep a couple of fallbacks to make local dev easier.
    """
    base = Path(__file__).resolve().parent
    candidates = [
        base / "assets" / "emails.json",              # Hosted bundle path
        base / "emails.json",                         # optional: next to the DAG
        base.parents[1] / "include" / "emails.json",  # legacy local path
    ]
    p = next((c for c in candidates if c.exists()), None)
    if not p:
        raise FileNotFoundError(
            "emails.json not found. Put it at dags/assets/emails.json."
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
    schedule=None,    # run on demand
    catchup=False,
    tags=["se-lab", "parallel", "emails"],
)
def se_email_parallel_enrichment():
    @task
    def sentiment_summary() -> Dict[str, Any]:
        """
        Toy “sentiment”: average body length → label.
        It’s arbitrary, but it gives us something to compute quickly.
        """
        rows = _load_emails()
        body_lens = [len((r.get("body") or r.get("text") or "")) for r in rows]
        avg_len = (sum(body_lens) / len(body_lens)) if body_lens else 0
        label = "pos" if avg_len > 80 else ("neg" if avg_len < 20 else "neutral")
        return {"avg_body_len": avg_len, "sentiment": label}

    @task
    def spam_score() -> Dict[str, Any]:
        """Count subjects that include “[SPAM]” as a crude spam indicator."""
        rows = _load_emails()
        spammy = sum(1 for r in rows if "[SPAM]" in (r.get("subject") or ""))
        return {"spam_count": spammy, "total": len(rows)}

    @task
    def entity_extract() -> Dict[str, Any]:
        """Count “@” characters in bodies as a stand-in for mentions/entities."""
        rows = _load_emails()
        mentions = sum((r.get("body") or r.get("text") or "").count("@") for r in rows)
        return {"entity_mentions": mentions}

    @task
    def thread_stats() -> Dict[str, Any]:
        """
        Group by subject and report the largest thread size and number of unique subjects.
        Assumes repeated subjects are part of the same thread.
        """
        rows = _load_emails()
        subjects: Dict[str, int] = {}
        for r in rows:
            s = r.get("subject") or ""
            subjects[s] = subjects.get(s, 0) + 1
        max_thread = max(subjects.values()) if subjects else 0
        return {"max_thread_length": max_thread, "unique_subjects": len(subjects)}

    # No dependencies: everything can run at once
    sentiment_summary()
    spam_score()
    entity_extract()
    thread_stats()


dag = se_email_parallel_enrichment()
