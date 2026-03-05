#!/usr/bin/env python
"""
Airflow DAG for Lochness summary metadata CSV creation.

This DAG generates site-level metadata CSV files for active subjects.
It can be configured to target specific projects and/or sites using DAG
parameters.

The metadata process:
1. Queries subjects that have required variables present
2. Builds legacy metadata rows for MindLAMP
3. Writes per-project/site metadata CSV files to PHOENIX paths
4. Logs operations and records generated files in the database

Parameters:
    - project_id: Optional project ID to limit metadata generation scope
    - site_id: Optional site ID to limit metadata generation scope
"""

from __future__ import annotations

import datetime

from airflow.sdk import Asset, DAG, Param
from airflow.sdk.definitions.param import ParamsDict
from airflow.providers.standard.operators.bash import BashOperator

# Define custom asset for generated metadata
metadata_csv_asset = Asset(
    uri="x-dpacc://summary-metadata-csv",
    name="Summary Metadata CSV",
)

# Trigger this DAG after REDCap metadata refresh is published.
redcap_metadata_refresh_asset = Asset(
    uri="x-dpacc://redcap-refresh-metadata",
    name="REDCap Metadata Refresh",
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
    dag_id="lochness_summary_create_metadata",
    dag_display_name="Summary Create Metadata",
    description="Generates metadata CSV files by project and site",
    doc_md=__doc__,
    default_args=default_args,
    schedule=[redcap_metadata_refresh_asset],
    max_active_runs=1,
    catchup=False,
    tags=["lochness", "summary", "metadata", "redcap"],
    params=ParamsDict(
        {
            "project_id": Param(
                default=None,
                type=["null", "string"],
                description=(
                    "Optional: Limit metadata generation to a specific "
                    "project ID. Leave empty for all projects."
                ),
                title="Project ID",
            ),
            "site_id": Param(
                default=None,
                type=["null", "string"],
                description=(
                    "Optional: Limit metadata generation to a specific "
                    "site ID. Leave empty for all sites."
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
echo "=================================="''',
        cwd="{{ var.value['LOCHNESS_REPO_ROOT'] }}",
    )

    summary_create_metadata = BashOperator(
        task_id="summary_create_metadata",
        task_display_name="Create Summary Metadata CSV",
        bash_command=(
            "{{ var.value['LOCHNESS_PYTHON_PATH'] }} "
            "{{ var.value['LOCHNESS_REPO_ROOT'] }}/lochness/summary/"
            "create_metadata_csv.py "
            "{% if params.project_id %} --project_id {{ params.project_id }}{% endif %} "
            "{% if params.site_id %} --site_id {{ params.site_id }}{% endif %}"
        ),
        cwd="{{ var.value['LOCHNESS_REPO_ROOT'] }}",
        outlets=[metadata_csv_asset],
    )

    # pylint: disable=pointless-statement
    print_info >> summary_create_metadata  # type: ignore[reportUnusedExpression]
