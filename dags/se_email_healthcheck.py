# dags/se_email_healthcheck.py
"""
Simple heartbeat DAG. It runs every 30 minutes to prove that the scheduler
and workers are alive. This is the place to hang a basic alert so you get
notified if the environment is unhealthy.
"""

from __future__ import annotations

import logging
from datetime import datetime

from airflow.decorators import dag, task


@dag(
    dag_id="se_email_healthcheck",
    start_date=datetime(2025, 1, 1),
    schedule="*/30 * * * *",  # every 30 minutes
    catchup=False,
    tags=["se-lab", "healthcheck"],
)
def se_email_healthcheck():
    @task
    def heartbeat():
        """
        Log a short message with the current timestamp. If this task stops
        succeeding on schedule, you’ll know something’s wrong with the env.
        """
        logging.info("✅ heartbeat ok @ %s", datetime.utcnow().isoformat() + "Z")

    heartbeat()


dag = se_email_healthcheck()
