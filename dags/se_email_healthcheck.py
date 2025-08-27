# dags/se_email_healthcheck.py
# Lightweight scheduled heartbeat DAG. Useful to demonstrate scheduling/monitoring.
# Airflow 3.x compatible.

from __future__ import annotations

from airflow.decorators import dag, task
from datetime import datetime


@dag(
    dag_id="se_email_healthcheck",
    start_date=datetime(2025, 1, 1),
    schedule="*/30 * * * *",  # every 30 minutes
    catchup=False,
    tags=["se-lab", "healthcheck", "emails"],
)
def se_email_healthcheck():
    @task
    def heartbeat() -> str:
        # Replace with an HTTP probe or a tiny warehouse freshness query if desired.
        return "ok"

    heartbeat()


dag = se_email_healthcheck()
