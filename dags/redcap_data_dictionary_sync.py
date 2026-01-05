#!/usr/bin/env python
"""
Airflow DAG for Lochness REDCap data dictionary synchronization.

This DAG synchronizes the REDCap data dictionary for all active REDCap data sources
in the database. It can be configured to target specific projects and/or sites
using DAG parameters.

The synchronization process:
1. Fetches the latest data dictionary from REDCap API
2. Compares with the stored version in the database
3. Updates the database if changes are detected
4. Logs all operations for audit trail

Parameters:
    - project_id: Optional project ID to limit sync scope
    - site_id: Optional site ID to limit sync scope
    - dry_run: When True, performs checks without updating the database
"""

from __future__ import annotations

import datetime

from airflow.sdk import Asset, DAG, Param
from airflow.sdk.definitions.param import ParamsDict
from airflow.providers.standard.operators.bash import BashOperator

# Define custom asset for REDCap data dictionary
redcap_data_dictionary_asset = Asset(
    uri="x-dpacc://redcap-data-dictionary",
    name="REDCap Data Dictionary",
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
    "execution_timeout": datetime.timedelta(minutes=30),
}

with DAG(
    dag_id="lochness_redcap_data_dictionary_sync",
    dag_display_name="REDCap Data Dictionary Sync",
    description="Synchronizes REDCap data dictionaries for all active data sources",
    doc_md=__doc__,
    default_args=default_args,
    schedule="10 8 * * *",  # Every day at 8:10 AM UTC -> 3:10 AM ET (after metadata refresh)
    max_active_runs=1,
    catchup=False,
    tags=["lochness", "redcap", "data-dictionary"],
    params=ParamsDict(
        {
            "project_id": Param(
                default=None,
                type=["null", "string"],
                description=(
                    "Optional: Limit sync to a specific project ID. "
                    "Leave empty to sync all projects."
                ),
                title="Project ID",
            ),
            "site_id": Param(
                default=None,
                type=["null", "string"],
                description=(
                    "Optional: Limit sync to a specific site ID. "
                    "Leave empty to sync all sites."
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

    redcap_data_dictionary_sync = BashOperator(
        task_id="redcap_data_dictionary_sync",
        task_display_name="Sync REDCap Data Dictionary",
        bash_command=(
            "{{ var.value['LOCHNESS_PYTHON_PATH'] }} "
            "{{ var.value['LOCHNESS_REPO_ROOT'] }}/lochness/sources/redcap/tasks/"
            "pull_dictionary.py "
            "{% if params.project_id %} --project_id {{ params.project_id }}{% endif %} "
            "{% if params.site_id %} --site_id {{ params.site_id }}{% endif %}"
        ),
        cwd="{{ var.value['LOCHNESS_REPO_ROOT'] }}",
        outlets=[redcap_data_dictionary_asset],
    )

    # pylint: disable=pointless-statement
    print_info >> redcap_data_dictionary_sync  # type: ignore[reportUnusedExpression]
