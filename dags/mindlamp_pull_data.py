#!/usr/bin/env python
"""
Airflow DAG for Lochness MindLAMP data pull.

This DAG pulls data from MindLAMP for all active MindLAMP data sources
in the database. It can be configured to target specific projects, sites, and
date ranges using DAG parameters.

The data pull process:
1. Authenticates with MindLAMP using stored credentials
2. Fetches sensor data, activity data, and other metrics for active subjects
3. Processes and stores the data in the file system
4. Supports both date range mode and days-based mode for flexible scheduling
5. Logs all operations for audit trail

Parameters:
    - project_id: Optional project ID to limit data pull scope
    - site_id: Optional site ID to limit data pull scope
    - start_date: Start date for data pull (date range mode)
    - end_date: End date for data pull (date range mode)
    - force_start_date: Start date for force redownload
    - force_end_date: End date for force redownload
    - days_to_pull: Number of days to pull (alternative to date range)
    - days_to_redownload: Number of days to redownload
"""

from __future__ import annotations

import datetime

from airflow.sdk import Asset, DAG, Param
from airflow.sdk.definitions.param import ParamsDict
from airflow.providers.standard.operators.bash import BashOperator

# Define custom asset for MindLAMP data
mindlamp_data_asset = Asset(
    uri="x-dpacc://mindlamp-data",
    name="MindLAMP Data",
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
    dag_id="lochness_mindlamp_pull_data",
    dag_display_name="MindLAMP Data Pull",
    description="Pulls data from MindLAMP for all active data sources",
    doc_md=__doc__,
    default_args=default_args,
    schedule="40 8 * * *",  # Every day at 8:40 AM UTC -> 3:40 AM ET
    max_active_runs=1,
    catchup=False,
    tags=["lochness", "mindlamp", "data-flow"],
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
            "start_date": Param(
                default=None,
                type=["null", "string"],
                format="date",
                description=(
                    "Start date for data pull (YYYY-MM-DD). "
                    "Leave empty to use days-based mode. "
                    "When specified, end_date must also be provided."
                ),
                title="Start Date",
            ),
            "end_date": Param(
                default=None,
                type=["null", "string"],
                format="date",
                description=(
                    "End date for data pull (YYYY-MM-DD). "
                    "Leave empty to use days-based mode. "
                    "When specified, start_date must also be provided."
                ),
                title="End Date",
            ),
            "force_start_date": Param(
                default=None,
                type=["null", "string"],
                format="date",
                description=(
                    "Start date for force redownload (YYYY-MM-DD). "
                    "Leave empty to use days_to_redownload parameter."
                ),
                title="Force Redownload Start Date",
            ),
            "force_end_date": Param(
                default=None,
                type=["null", "string"],
                format="date",
                description=(
                    "End date for force redownload (YYYY-MM-DD). "
                    "Leave empty to use days_to_redownload parameter."
                ),
                title="Force Redownload End Date",
            ),
            "days_to_pull": Param(
                default=14,
                type=["null", "integer"],
                minimum=1,
                maximum=365,
                description=(
                    "Number of days of data to pull (used when start_date/end_date not specified). "
                    "Default is 14 days."
                ),
                title="Days to Pull",
            ),
            "days_to_redownload": Param(
                default=7,
                type=["null", "integer"],
                minimum=1,
                maximum=365,
                description=(
                    "Number of days of recent data to redownload (used when force dates not specified). "
                    "Default is 7 days."
                ),
                title="Days to Redownload",
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
echo "Start Date: {{ params.start_date or 'AUTO' }}"
echo "End Date: {{ params.end_date or 'AUTO' }}"
echo "Force Start Date: {{ params.force_start_date or 'AUTO' }}"
echo "Force End Date: {{ params.force_end_date or 'AUTO' }}"
echo "Days to Pull: {{ params.days_to_pull }}"
echo "Days to Redownload: {{ params.days_to_redownload }}"
echo "=================================="''',
        cwd="{{ var.value['LOCHNESS_REPO_ROOT'] }}",
    )

    mindlamp_pull_data = BashOperator(
        task_id="mindlamp_pull_data",
        task_display_name="Pull MindLAMP Data",
        bash_command=(
            "{{ var.value['LOCHNESS_PYTHON_PATH'] }} "
            "{{ var.value['LOCHNESS_REPO_ROOT'] }}/lochness/sources/mindlamp/tasks/"
            "pull_data.py "
            "{% if params.project_id %} --project_id {{ params.project_id }}{% endif %} "
            "{% if params.site_id %} --site_id {{ params.site_id }}{% endif %} "
            "{% if params.start_date %} --start_date {{ params.start_date }}{% endif %} "
            "{% if params.end_date %} --end_date {{ params.end_date }}{% endif %} "
            "{% if params.force_start_date %} --force_start_date {{ params.force_start_date }}{% endif %} "
            "{% if params.force_end_date %} --force_end_date {{ params.force_end_date }}{% endif %} "
            "{% if params.days_to_pull %} --days_to_pull {{ params.days_to_pull }}{% endif %} "
            "{% if params.days_to_redownload %} --days_to_redownload {{ params.days_to_redownload }}{% endif %}"
        ),
        cwd="{{ var.value['LOCHNESS_REPO_ROOT'] }}",
        outlets=[mindlamp_data_asset],
    )

    # pylint: disable=pointless-statement
    print_info >> mindlamp_pull_data  # type: ignore[reportUnusedExpression]
