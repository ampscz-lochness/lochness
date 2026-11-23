#!/usr/bin/env python
"""
Airflow DAG for Lochness Terra data pull.

This DAG pulls data from Terra (app.terra.bio on Google Cloud Platform) for all
active Terra data sources in the database. It can be configured to target
specific projects, sites, and/or data sources using DAG parameters.

The data pull process:
1. Authenticates with Terra & Google Cloud Storage using stored Service Account
   credentials (or Application Default Credentials)
2. Fetches workspace entity data tables and/or GCS files referenced by gs:// URIs
   or stored under subject folders in the workspace GCS bucket
3. Saves new or updated tables and files atomically in the PHOENIX structure
4. Records File and DataPull entries and logs all operations for audit trail
5. Triggers `lochness_push_data` (scoped to the same project/site) and waits
   for it to complete, then emits the `Terra Data Pushed` asset

Parameters:
    - project_id: Optional project ID to limit data pull scope
    - site_id: Optional site ID to limit data pull scope
    - source_id: Optional Terra data source name to limit scope
    - dry_run: When True, checks connectivity and logs actions without downloading
    - force_redownload: When True, ignores version caches and redownloads files
"""

from __future__ import annotations

import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import DAG, Asset, Param
from airflow.sdk.definitions.param import ParamsDict

# Define custom assets for Terra data
terra_data_asset = Asset(
    uri="x-dpacc://terra-data",
    name="Terra Data",
)
terra_data_pushed_asset = Asset(
    uri="x-dpacc://terra-data-pushed",
    name="Terra Data Pushed",
)

# Define variables
default_args = {
    "owner": "ctdpacc",
    "depends_on_past": False,
    "start_date": datetime.datetime(2026, 1, 1),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": datetime.timedelta(minutes=1),
    "execution_timeout": datetime.timedelta(minutes=180),
}

with DAG(
    dag_id="lochness_terra_pull_data",
    dag_display_name="Terra Data Pull",
    description="Pulls entity tables and GCS files from Terra for all active data sources",
    doc_md=__doc__,
    default_args=default_args,
    schedule="50 8 * * *",  # Every day at 8:50 AM UTC -> 3:50 AM ET
    max_active_runs=1,
    catchup=False,
    tags=["lochness", "terra", "data-flow"],
    params=ParamsDict(
        {
            "project_id": Param(
                default=None,
                type=["null", "string"],
                description=(
                    "Optional: Limit data pull to a specific project ID. "
                    "Leave empty to pull data for all projects."
                ),
                title="Project ID",
            ),
            "site_id": Param(
                default=None,
                type=["null", "string"],
                description=(
                    "Optional: Limit data pull to a specific site ID. "
                    "Leave empty to pull data for all sites."
                ),
                title="Site ID",
            ),
            "source_id": Param(
                default=None,
                type=["null", "string"],
                description=(
                    "Optional: Limit data pull to a specific Terra data source name. "
                    "Leave empty to pull data for all Terra data sources."
                ),
                title="Terra Source ID",
            ),
            "dry_run": Param(
                default=False,
                type=["null", "boolean"],
                description=(
                    "When enabled, validates connectivity and logs planned downloads "
                    "without writing files."
                ),
                title="Dry Run Mode",
            ),
            "force_redownload": Param(
                default=False,
                type=["null", "boolean"],
                description=(
                    "When enabled, ignores cached remote version checks and "
                    "redownloads matching tables and files."
                ),
                title="Force Redownload",
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
echo "Source ID: {{ params.source_id or 'ALL' }}"
echo "Dry Run: {{ params.dry_run }}"
echo "Force Redownload: {{ params.force_redownload }}"
echo "=================================="''',
        cwd="{{ var.value['LOCHNESS_REPO_ROOT'] }}",
    )

    terra_pull_data = BashOperator(
        task_id="terra_pull_data",
        task_display_name="Pull Terra Data",
        bash_command=(
            "{{ var.value['LOCHNESS_PYTHON_PATH'] }} "
            "{{ var.value['LOCHNESS_REPO_ROOT'] }}/lochness/sources/terra/tasks/"
            "pull_data.py "
            "{% if params.project_id %} --project_id {{ params.project_id }}{% endif %} "
            "{% if params.site_id %} --site_id {{ params.site_id }}{% endif %} "
            "{% if params.source_id %} --source_id {{ params.source_id }}{% endif %} "
            "{% if params.dry_run %} --dry_run{% endif %} "
            "{% if params.force_redownload %} --force_redownload{% endif %}"
        ),
        cwd="{{ var.value['LOCHNESS_REPO_ROOT'] }}",
        outlets=[terra_data_asset],
    )

    trigger_push_data = TriggerDagRunOperator(
        task_id="trigger_push_data",
        task_display_name="Trigger Push Data",
        trigger_dag_id="lochness_push_data",
        conf={
            "project_id": "{{ params.project_id or '' }}",
            "site_id": "{{ params.site_id or '' }}",
        },
        wait_for_completion=True,
        poke_interval=60,
        allowed_states=["success"],
        failed_states=["failed"],
        retries=0,  # a retry would trigger a second push run
        outlets=[terra_data_pushed_asset],
    )

    # pylint: disable=pointless-statement
    print_info >> terra_pull_data >> trigger_push_data  # type: ignore[reportUnusedExpression]
