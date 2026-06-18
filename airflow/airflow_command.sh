#!/usr/bin/env bash
set -euo pipefail

DAG_ID="adam_simple_function_dag"
TASK_ID="stage1"
DATE="$(date +%Y-%m-%d)"

echo "DAG ID: $DAG_ID"
echo "Execution date: $DATE"

# Uncomment the command you want to run.

# Test DAG locally with a logical execution date
airflow dags test "$DAG_ID" "$DATE"

# Check DAG run state for the given date
airflow dags state "$DAG_ID" "$DATE"

# List DAG runs
# airflow dags list-runs -d "$DAG_ID"

# Trigger DAG
# airflow dags trigger "$DAG_ID"

# Check task state for a specific task
# airflow tasks state "$DAG_ID" "$TASK_ID" "$DATE"

# Test task locally
# airflow tasks test "$DAG_ID" "$TASK_ID" "$DATE"
