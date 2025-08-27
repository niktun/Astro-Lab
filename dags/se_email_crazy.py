# dags/se_email_crazy.py
"""
This DAG is a visual stress test for Airflow's Graph view. It doesn't do any
real work; it just builds a giant web of tiny tasks so you can see lots of
edges, branches, joins, and task groups all at once.

What you'll see:
- Many "shard" groups that each look like a tiny pipeline
- A cross-shard mesh so lines cut across groups
- A global fan‑in to a few stages, then a big fan‑out
- Classic branch-and-join diamonds
- A final aggregation funnel into a single "finished" task

Why I built it: demo concurrency, task grouping, branching, and how gnarly
things can look when you wire lots of independent paths together.
"""

from __future__ import annotations

from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime

# How many nodes/edges to draw; bump these to make it even busier
N_SHARDS = 20      # number of shard groups
M_FANOUT = 12      # count of fan-out tasks after global stages
M_VALIDATE = 10    # number of validator tasks after the fan-out


def _branch_choice_factory(idx: int):
    """For each branch block, pick one of two paths.

    This is deterministic so both paths exist in the graph. I alternate
    run/skip by index so the diagram shows a diamond and a join either way.
    """
    def _choose():
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
    # Single entry/exit points so the whole spaghetti ball is connected
    kick_off = EmptyOperator(task_id="kick_off")
    finished = EmptyOperator(task_id="finished")

    # Build N shard groups. Each shard is a tiny 6‑step pipeline plus a checksum edge.
    # I keep references to the "dedupe" and "qa" tasks to wire the global mesh later.
    shard_qa_tasks = []
    shard_dedupe_tasks = []

    for i in range(1, N_SHARDS + 1):
        with TaskGroup(group_id=f"shard_{i:02d}") as tg:
            s_start = EmptyOperator(task_id="start")
            fetch = EmptyOperator(task_id="fetch")
            checksum = EmptyOperator(task_id="checksum")
            parse = EmptyOperator(task_id="parse")
            normalize = EmptyOperator(task_id="normalize")
            dedupe = EmptyOperator(task_id="dedupe")
            qa = EmptyOperator(task_id="qa")

            # Internal flow: start → fetch → parse → normalize → dedupe → qa
            # Plus: checksum also feeds normalize so we get extra lines.
            s_start >> [fetch, checksum]
            fetch >> parse >> normalize >> dedupe >> qa
            checksum >> normalize

        # Everything hangs off kick_off so the graph has a single root
        kick_off >> s_start

        shard_qa_tasks.append(qa)
        shard_dedupe_tasks.append(dedupe)

    # Cross‑shard mesh: each shard's "dedupe" points to the next few shards' "qa"
    # (forward‑only to avoid cycles). This draws a bunch of diagonal edges.
    for i in range(N_SHARDS):
        for j in range(i + 1, min(N_SHARDS, i + 4)):
            shard_dedupe_tasks[i] >> shard_qa_tasks[j]

    # Global fan‑in: every shard QA feeds a small set of stages (a..e)
    stages = [EmptyOperator(task_id=f"stage_{letter}") for letter in list("abcde")]
    for qa in shard_qa_tasks:
        for st in stages:
            qa >> st

    # Add three branch‑and‑join diamonds to make the paths more interesting
    joins = []
    for idx, st in enumerate(stages[:3], start=1):
        chooser = BranchPythonOperator(task_id=f"branch_{idx}", python_callable=_branch_choice_factory(idx))
        run = EmptyOperator(task_id=f"path_run_{idx}")
        skip = EmptyOperator(task_id=f"path_skip_{idx}")
        # Join should succeed if at least one branch succeeded
        join = EmptyOperator(task_id=f"join_{idx}", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
        st >> chooser >> [run, skip]
        run >> join
        skip >> join
        joins.append(join)

    # Big fan‑out: stages and joins both explode to a bunch of parallel tasks
    fanouts = [EmptyOperator(task_id=f"fanout_{k:02d}") for k in range(1, M_FANOUT + 1)]
    for st in stages:
        for fo in fanouts:
            st >> fo
    for jn in joins:
        for fo in fanouts:
            jn >> fo

    # Validators hang off every fanout task to multiply edges further
    validators = [EmptyOperator(task_id=f"validate_{k:02d}") for k in range(1, M_VALIDATE + 1)]
    for fo in fanouts:
        for val in validators:
            fo >> val

    # Multi‑stage aggregation so the graph funnels back down to a single node
    agg_1 = EmptyOperator(task_id="aggregate_1")
    agg_2 = EmptyOperator(task_id="aggregate_2")
    agg_3 = EmptyOperator(task_id="aggregate_3")

    for val in validators:
        val >> agg_1
        val >> agg_2
    agg_1 >> agg_3
    agg_2 >> agg_3

    # Done.
    agg_3 >> finished


crazy_dag = se_email_crazy()
