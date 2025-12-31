#!/usr/bin/env python
"""
Airflow DAG for Lochness CANTAB data flow.

This DAG handles the complete CANTAB data workflow:
1. Sync CANTAB subject IDs - Links study subject IDs to their CANTAB IDs
2. Pull CANTAB data - Fetches test results and session data from CANTAB API

The workflow can be configured to target specific projects and/or sites
using DAG parameters.

The process:
1. Links study subjects to their CANTAB IDs by querying the CANTAB API
2. Fetches the latest test results and session data from CANTAB
3. Stores the data in the database for further processing
4. Logs all operations for audit trail

Parameters:
    - project_id: Optional project ID to limit workflow scope
    - site_id: Optional site ID to limit workflow scope
    - dry_run: When True, performs checks without updating the database
"""

from __future__ import annotations

import datetime

from airflow.sdk import Asset, DAG, Param
from airflow.sdk.definitions.param import ParamsDict
from airflow.providers.standard.operators.bash import BashOperator

# Define custom asset for CANTAB data
cantab_data_asset = Asset(
    uri="x-dpacc://cantab-data",
    name="CANTAB Data",
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
    "execution_timeout": datetime.timedelta(minutes=45),
}

with DAG(
    dag_id="lochness_cantab_data_flow",
    dag_display_name="CANTAB Data Flow",
    description="Complete CANTAB data workflow: sync subject IDs and pull test data",
    doc_md=__doc__,
    default_args=default_args,
    schedule="50 8 * * *",  # Every day at 8:50 AM UTC -> 3:50 AM ET
    max_active_runs=1,
    catchup=False,
    tags=["lochness", "cantab", "data-flow"],
    params=ParamsDict(
        {
            "project_id": Param(
                default=None,
                type=["null", "string"],
                description=(
                    "Optional: Limit workflow to a specific project ID. "
                    "Leave empty to process all projects."
                ),
                title="Project ID",
            ),
            "site_id": Param(
                default=None,
                type=["null", "string"],
                description=(
                    "Optional: Limit workflow to a specific site ID. "
                    "Leave empty to process all sites."
                ),
                title="Site ID",
            ),
            "dry_run": Param(
                default=False,
                type=["null", "boolean"],
                description=(
                    "When enabled, performs validation checks "
                    "without updating the database."
                ),
                title="Dry Run Mode",
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
echo "Dry Run: {{ params.dry_run }}"
echo "=================================="''',
        cwd="{{ var.value['LOCHNESS_REPO_ROOT'] }}",
    )

    cantab_sync_subject_ids = BashOperator(
        task_id="cantab_sync_subject_ids",
        task_display_name="Sync CANTAB Subject IDs",
        bash_command=(
            "{{ var.value['LOCHNESS_PYTHON_PATH'] }} "
            "{{ var.value['LOCHNESS_REPO_ROOT'] }}/lochness/sources/cantab/tasks/"
            "sync.py "
            "{% if params.project_id %} --project_id {{ params.project_id }}{% endif %} "
            "{% if params.site_id %} --site_id {{ params.site_id }}{% endif %}"
        ),
        cwd="{{ var.value['LOCHNESS_REPO_ROOT'] }}",
    )

    cantab_pull_data = BashOperator(
        task_id="cantab_pull_data",
        task_display_name="Pull CANTAB Data",
        bash_command=(
            "{{ var.value['LOCHNESS_PYTHON_PATH'] }} "
            "{{ var.value['LOCHNESS_REPO_ROOT'] }}/lochness/sources/cantab/tasks/"
            "pull_data.py "
            "{% if params.project_id %} --project_id {{ params.project_id }}{% endif %} "
            "{% if params.site_id %} --site_id {{ params.site_id }}{% endif %}"
        ),
        cwd="{{ var.value['LOCHNESS_REPO_ROOT'] }}",
        outlets=[cantab_data_asset],
    )

    # pylint: disable=pointless-statement
    print_info >> cantab_sync_subject_ids >> cantab_pull_data  # type: ignore[reportUnusedExpression]
