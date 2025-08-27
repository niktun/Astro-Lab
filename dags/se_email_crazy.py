# dags/se_email_crazy.py
# "Crazy" DAG designed to produce a very dense Graph View with lots of lines/edges.
# Airflow 3.x compatible. Purely synthetic (fast no-op tasks) but visually complex.
# Highlights:
# - 20 shard TaskGroups with 6+ tasks each
# - Cross-shard mesh edges to create many interconnections
# - Global fan-in / fan-out stages
# - Branching with joins
# - Final multi-stage aggregation

from __future__ import annotations

from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime

N_SHARDS = 20               # number of shard groups (increase to 30 if you want *even* more lines)
M_FANOUT = 12               # number of global fan-out nodes
M_VALIDATE = 10             # number of global validators


def _branch_choice_factory(idx: int):
    # Simple deterministic branch so the graph renders both paths
    def _choose():
        # Alternate between run/skip to show both paths in the graph
        return f"path_run_{idx}" if (idx % 2 == 0) else f"path_skip_{idx}"
    return _choose


@dag(
    dag_id="se_email_crazy",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["se-lab", "crazy", "mesh", "demo"],
)
def se_email_crazy():
    # Root start and end sentinels
    kick_off = EmptyOperator(task_id="kick_off")
    finished = EmptyOperator(task_id="finished")

    # 1) Shard groups: each with a small internal pipeline
    shard_qa_tasks = []     # collect final node per shard to wire up fan-in stages
    shard_dedupe_tasks = [] # collect mid nodes to create cross-shard mesh later

    for i in range(1, N_SHARDS + 1):
        with TaskGroup(group_id=f"shard_{i:02d}") as tg:
            s_start = EmptyOperator(task_id="start")
            fetch = EmptyOperator(task_id="fetch")
            checksum = EmptyOperator(task_id="checksum")
            parse = EmptyOperator(task_id="parse")
            normalize = EmptyOperator(task_id="normalize")
            dedupe = EmptyOperator(task_id="dedupe")
            qa = EmptyOperator(task_id="qa")

            # Internal wiring (adds a couple of cross-deps for extra lines)
            s_start >> [fetch, checksum]
            fetch >> parse >> normalize >> dedupe >> qa
            checksum >> normalize

        # Connect the shard to the global kick-off to ensure everything is reachable
        kick_off >> s_start

        # Track key nodes for later global wiring
        shard_qa_tasks.append(qa)
        shard_dedupe_tasks.append(dedupe)

    # 2) Cross-shard mesh: connect each shard's dedupe to the next few shards' QA
    for i in range(N_SHARDS):
        for j in range(i + 1, min(N_SHARDS, i + 4)):  # connect forward only to avoid cycles
            shard_dedupe_tasks[i] >> shard_qa_tasks[j]

    # 3) Global fan-in stages (A..E) fed by *all* shard QA tasks
    stages = [EmptyOperator(task_id=f"stage_{letter}") for letter in list("abcde")]
    # All shard QA -> all stages (lots of edges)
    for qa in shard_qa_tasks:
        for st in stages:
            qa >> st

    # 4) Branches off stage_a, stage_b, stage_c to show alternative paths and joins
    joins = []
    for idx, st in enumerate(stages[:3], start=1):
        chooser = BranchPythonOperator(task_id=f"branch_{idx}", python_callable=_branch_choice_factory(idx))
        run = EmptyOperator(task_id=f"path_run_{idx}")
        skip = EmptyOperator(task_id=f"path_skip_{idx}")
        join = EmptyOperator(task_id=f"join_{idx}", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
        st >> chooser >> [run, skip]  # branch to two options
        run >> join
        skip >> join
        joins.append(join)

    # 5) Fan-out: after stages + joins, explode to many parallel tasks
    fanouts = [EmptyOperator(task_id=f"fanout_{k:02d}") for k in range(1, M_FANOUT + 1)]
    # Wire stages and joins to all fanout nodes
    for st in stages:
        for fo in fanouts:
            st >> fo
    for jn in joins:
        for fo in fanouts:
            jn >> fo

    # 6) Validators: many downstream checks for more edges
    validators = [EmptyOperator(task_id=f"validate_{k:02d}") for k in range(1, M_VALIDATE + 1)]
    for fo in fanouts:
        for val in validators:
            fo >> val

    # 7) Multi-stage aggregation
    agg_1 = EmptyOperator(task_id="aggregate_1")
    agg_2 = EmptyOperator(task_id="aggregate_2")
    agg_3 = EmptyOperator(task_id="aggregate_3")

    for val in validators:
        val >> agg_1
        val >> agg_2
    agg_1 >> agg_3
    agg_2 >> agg_3

    # 8) Finalize
    agg_3 >> finished


crazy_dag = se_email_crazy()
