#!/usr/bin/env python
"""
Airflow DAG for Lochness SharePoint data pull.

This DAG pulls data from SharePoint for all active SharePoint data sources
in the database. It can be configured to target specific projects, sites, and/or
data sources using DAG parameters.

The data pull process:
1. Authenticates with SharePoint using stored credentials
2. Navigates through the SharePoint folder structure for each data source
3. Downloads new or updated files for all active subjects
4. Organizes files in the PHOENIX structure for further processing
5. Logs all operations for audit trail

Parameters:
    - project_id: Optional project ID to limit data pull scope
    - site_id: Optional site ID to limit data pull scope
    - source_id: Optional SharePoint data source ID to limit scope
    - dry_run: When True, performs checks without downloading files
"""

from __future__ import annotations

import datetime

from airflow.sdk import Asset, DAG, Param
from airflow.sdk.definitions.param import ParamsDict
from airflow.providers.standard.operators.bash import BashOperator

# Define custom asset for SharePoint data
sharepoint_data_asset = Asset(
    uri="x-dpacc://sharepoint-data",
    name="SharePoint Data",
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
    dag_id="procan_sharepoint_pull_data",
    dag_display_name="SharePoint Data Pull",
    description="Pulls data from SharePoint for all active data sources",
    doc_md=__doc__,
    default_args=default_args,
    schedule="30 8 * * *",  # Every day at 8:30 AM UTC -> 3:30 AM ET
    max_active_runs=1,
    catchup=False,
    tags=["lochness", "sharepoint", "data-flow"],
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
                    "Optional: Limit data pull to a specific SharePoint data source ID. "
                    "Leave empty to pull data for all data sources."
                ),
                title="SharePoint Source ID",
            ),
            "dry_run": Param(
                default=False,
                type=["null", "boolean"],
                description=(
                    "When enabled, performs validation checks "
                    "without downloading files."
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
echo "Source ID: {{ params.source_id or 'ALL' }}"
echo "Dry Run: {{ params.dry_run }}"
echo "=================================="''',
        cwd="{{ var.value['LOCHNESS_REPO_ROOT'] }}",
    )

    sharepoint_pull_data = BashOperator(
        task_id="sharepoint_pull_data",
        task_display_name="Pull SharePoint Data",
        bash_command=(
            "{{ var.value['LOCHNESS_PYTHON_PATH'] }} "
            "{{ var.value['LOCHNESS_REPO_ROOT'] }}/lochness/sources/sharepoint/tasks/"
            "pull_data.py "
            "{% if params.project_id %} --project_id {{ params.project_id }}{% endif %} "
            "{% if params.site_id %} --site_id {{ params.site_id }}{% endif %} "
            "{% if params.source_id %} --source_id {{ params.source_id }}{% endif %}"
        ),
        cwd="{{ var.value['LOCHNESS_REPO_ROOT'] }}",
        outlets=[sharepoint_data_asset],
    )

    # pylint: disable=pointless-statement
    print_info >> sharepoint_pull_data  # type: ignore[reportUnusedExpression]
