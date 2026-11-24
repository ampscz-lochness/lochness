"""
SupportedDataSourceTypes Model

This model defines the supported data source types for Lochness,
and defined their metadata dictionaries.
"""

from typing import Any, Dict, List

from pydantic import BaseModel

from lochness.helpers import db


class SupportedDataSourceTypes(BaseModel):
    """
    Supported data source types for Lochness.
    """

    data_source_type: str
    data_source_metadata_dict: Dict[str, Any]

    @staticmethod
    def init_db_table_query() -> str:
        """
        Returns the SQL query to create the database table for supported data source types.
        """
        sql_query = """
            CREATE TABLE supported_data_source_types (
                data_source_type TEXT NOT NULL,
                data_source_metadata_dict JSONB,
                PRIMARY KEY (data_source_type)
            );
        """

        return sql_query

    @staticmethod
    def drop_db_table_query() -> str:
        """
        Returns the SQL query to drop the database table for supported data source types.
        """
        sql_query = """
            DROP TABLE IF EXISTS supported_data_source_types;
        """

        return sql_query

    def to_sql_query(self) -> str:
        """
        Converts the SupportedDataSourceTypes instance to a SQL insert statement.
        """
        sql = f"""
            INSERT INTO supported_data_source_types (
                data_source_type, data_source_metadata_dict
            ) VALUES (
                '{self.data_source_type}', '{db.sanitize_json(self.data_source_metadata_dict)}'
            ) ON CONFLICT (data_source_type)
            DO UPDATE SET data_source_metadata_dict = EXCLUDED.data_source_metadata_dict;
        """
        return sql


SUPPORTED_DATA_SOURCE_TYPES: List[SupportedDataSourceTypes] = [
    SupportedDataSourceTypes(
        data_source_type="redcap",
        data_source_metadata_dict={
            "keystore_name": "Name of the keystore entry containing the API token",
            "endpoint_url": "REDCap API endpoint URL",
            "subject_id_variable": "Name of the variable containing subject IDs",
            "optional_variables_dictionary": "List of additional variables to fetch",
        },
    ),
    SupportedDataSourceTypes(
        data_source_type="mindlamp",
        data_source_metadata_dict={
            "api_url": "Mindlamp API URL",
            "keystore_name": "Name of the keystore entry containing the credentials",
        },
    ),
    SupportedDataSourceTypes(
        data_source_type="cantab",
        data_source_metadata_dict={
            "api_url": "CANTAB API URL",
            "keystore_name": "Name of the keystore entry containing the credentials",
        },
    ),
    SupportedDataSourceTypes(
        data_source_type="sharepoint",
        data_source_metadata_dict={
            "keystore_name": "Name of the keystore entry containing the API token",
            "site_url": "Site URL",
            "form_name": "Name of the Teams Form",
        },
    ),
    SupportedDataSourceTypes(
        data_source_type="terra",
        data_source_metadata_dict={
            "keystore_name": "Name of the keystore entry containing Terra B2C (Microsoft) refresh token JSON or GCP Service Account JSON (optional for ADC)",
            "workspace_namespace": "Terra billing project / workspace namespace",
            "workspace_name": "Terra workspace name",
            "modality": "PHOENIX modality subdirectory (default: terra)",
            "api_url": "Terra/FireCloud API base URL (default: https://api.firecloud.org)",
            "pull_tables": "Whether to pull workspace entity data tables (default: true)",
            "pull_table_gcs_files": "Whether to download gs:// files referenced in entity tables (default: true)",
            "pull_bucket_files": "Whether to sync files directly from subject folders in the workspace GCS bucket (default: false)",
            "pull_workspace_bucket": "Whether to mirror all blobs from the workspace GCS bucket under the target subject (default: false)",
            "entity_types": "Optional list of Terra entity table names to pull (default: all)",
            "table_format": "Output format for entity tables: json, tsv, or csv (default: json)",
            "subject_id_column": "Entity field or attribute matching subject ID (default: name)",
            "subject_id_regex": "Optional regex (with optional capture group 1) to normalize extracted subject IDs",
            "fallback_subject_id": "Optional cohort subject ID for entity rows without a per-subject match",
            "subject_id_metadata_key": "Optional key in subject_metadata for mapping Lochness subject to Terra ID",
            "gcs_uri_columns": "Optional list of table columns containing gs:// URIs (default: auto-detect)",
            "bucket_name": "Optional GCS bucket name override (default: workspace bucketName)",
            "bucket_prefix": "Optional GCS bucket prefix for direct bucket sync",
            "bucket_subject_folder_template": "Subfolder pattern under bucket_prefix (default: {subject_id})",
            "include_patterns": "Optional list of glob patterns for GCS files to include",
            "exclude_patterns": "Optional list of glob patterns for GCS files to exclude",
            "billing_project": "Optional GCP billing project for Requester Pays GCS buckets",
            "max_workers": "Number of parallel worker threads for GCS metadata/downloads (default: 16)",
        },
    ),
]


def populate_supported_data_source_types() -> List[str]:
    """
    Returns SQL queries to populate the supported data source types table.
    """
    sql_queries: List[str] = []
    for data_source_type in SUPPORTED_DATA_SOURCE_TYPES:
        sql_queries.append(data_source_type.to_sql_query())

    return sql_queries
