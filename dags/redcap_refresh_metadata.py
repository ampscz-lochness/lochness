#!/usr/bin/env python
"""
Airflow DAG for Lochness REDCap metadata refresh.

This DAG refreshes the REDCap metadata for all active REDCap data sources
in the database. It can be configured to target specific projects and/or sites
using DAG parameters.

The metadata refresh process:
1. Fetches the latest records from REDCap API
2. Filters records by site ID
3. Extracts metadata from specified variables
4. Updates the database with new/updated subject metadata
5. Logs all operations for audit trail

Parameters:
    - project_id: Optional project ID to limit refresh scope
    - site_id: Optional site ID to limit refresh scope
"""

from __future__ import annotations

import datetime

from airflow.sdk import DAG, Param
from airflow.sdk.definitions.param import ParamsDict
from airflow.providers.standard.operators.bash import BashOperator

# Define variables
default_args = {
    "owner": "ctdpacc",
    "depends_on_past": False,
    "start_date": datetime.datetime(2025, 12, 19),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": datetime.timedelta(minutes=1),
    "execution_timeout": datetime.timedelta(minutes=30),
}

with DAG(
    dag_id="lochness_refresh_metadata",
    dag_display_name="REDCap Metadata Refresh",
    description="Refreshes REDCap metadata for all active data sources",
    doc_md=__doc__,
    default_args=default_args,
    schedule="0 8 * * *",  # Every day at 8:00 AM UTC -> 3:00 AM ET (runs first)
    max_active_runs=1,
    catchup=False,
    tags=["lochness", "redcap", "metadata"],
    params=ParamsDict(
        {
            "project_id": Param(
                default="",
                type=["null", "string"],
                description=(
                    "Optional: Limit refresh to a specific project ID. "
                    "Leave empty to refresh all projects."
                ),
                title="Project ID",
            ),
            "site_id": Param(
                default="",
                type=["null", "string"],
                description=(
                    "Optional: Limit refresh to a specific site ID. "
                    "Leave empty to refresh all sites."
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

    refresh_metadata = BashOperator(
        task_id="refresh_metadata",
        task_display_name="Refresh REDCap Metadata",
        bash_command=(
            "{{ var.value['LOCHNESS_PYTHON_PATH'] }} "
            "{{ var.value['LOCHNESS_REPO_ROOT'] }}/lochness/sources/redcap/tasks/"
            "refresh_metadata.py "
            "{% if params.project_id %} --project_id {{ params.project_id }}{% endif %} "
            "{% if params.site_id %} --site_id {{ params.site_id }}{% endif %}"
        ),
        cwd="{{ var.value['LOCHNESS_REPO_ROOT'] }}",
    )

    # pylint: disable=pointless-statement
    print_info >> refresh_metadata  # type: ignore[reportUnusedExpression]
