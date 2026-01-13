#!/usr/bin/env python
"""
Airflow DAG for Lochness data push.

This DAG pushes data from the local file system to all configured data sinks
in the database. It can be configured to target specific projects and/or sites
using DAG parameters.

The data push process:
1. Retrieves all active data sinks matching the project/site criteria
2. Identifies files that need to be pushed (not yet pushed to the sink)
3. Pushes each file to the appropriate data sink
4. Records successful pushes in the data_pushes table
5. Logs all operations for audit trail

Parameters:
    - project_id: Project ID to push data for (required)
    - site_id: Site ID to push data for (required)
"""

from __future__ import annotations

import datetime

from airflow.sdk import Asset, DAG, Param
from airflow.sdk.definitions.param import ParamsDict
from airflow.providers.standard.operators.bash import BashOperator

# Define custom asset for pushed data
pushed_data_asset = Asset(
    uri="x-dpacc://pushed-data",
    name="Pushed Data",
)

# Define variables
default_args = {
    "owner": "ctdpacc",
    "depends_on_past": False,
    "start_date": datetime.datetime(2025, 12, 19),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": datetime.timedelta(minutes=1),
    "execution_timeout": datetime.timedelta(minutes=60),
}

with DAG(
    dag_id="lochness_push_data",
    dag_display_name="Push Data to Sinks",
    description="Pushes data from local file system to configured data sinks",
    doc_md=__doc__,
    default_args=default_args,
    schedule="0 10 * * *",  # Every day at 10:00 AM UTC -> 5:00 AM ET (runs after data pulls)
    max_active_runs=1,
    catchup=False,
    tags=["lochness", "data-push", "data-flow"],
    params=ParamsDict(
        {
            "project_id": Param(
                default=None,
                type=["null", "string"],
                description=(
                    "Optional: Project ID to push data for. "
                    "Leave empty to push data for all projects."
                ),
                title="Project ID",
            ),
            "site_id": Param(
                default=None,
                type=["null", "string"],
                description=(
                    "Optional: Site ID to push data for. "
                    "Leave empty to push data for all sites."
                ),
                title="Site ID",
            ),
        }
    ),
) as dag:

    print_info = BashOperator(
        task_id="print_info",
        task_display_name="Print Environment Info",
        bash_command='''echo "===== Environment Information ====="
echo "$(date) - Hostname: $(hostname)"
echo "$(date) - User: $(whoami)"
echo ""
echo "===== Repository Status ====="
echo "$(date) - Current directory: $(pwd)"
echo "$(date) - Git branch: $(git rev-parse --abbrev-ref HEAD)"
echo "$(date) - Git commit: $(git rev-parse HEAD)"
echo "$(date) - Git status: "
git status --porcelain
echo ""
echo "===== System Status ====="
echo "$(date) - Uptime: $(uptime)"
echo ""
echo "===== DAG Parameters ====="
echo "LOCHNESS_REPO_ROOT: {{ var.value['LOCHNESS_REPO_ROOT'] }}"
echo "LOCHNESS_PYTHON_PATH: {{ var.value['LOCHNESS_PYTHON_PATH'] }}"
echo "Project ID: {{ params.project_id or 'ALL' }}"
echo "Site ID: {{ params.site_id or 'ALL' }}"
echo "================================="''',
        cwd="{{ var.value['LOCHNESS_REPO_ROOT'] }}",
    )

    push_data = BashOperator(
        task_id="push_data",
        task_display_name="Push Data to Sinks",
        bash_command=(
            "{{ var.value['LOCHNESS_PYTHON_PATH'] }} "
            "{{ var.value['LOCHNESS_REPO_ROOT'] }}/lochness/tasks/push_data.py "
            "{% if params.project_id %} --project_id {{ params.project_id }}{% endif %} "
            "{% if params.site_id %} --site_id {{ params.site_id }}{% endif %}"
        ),
        cwd="{{ var.value['LOCHNESS_REPO_ROOT'] }}",
        outlets=[pushed_data_asset],
    )

    # pylint: disable=pointless-statement
    print_info >> push_data  # type: ignore[reportUnusedExpression]
