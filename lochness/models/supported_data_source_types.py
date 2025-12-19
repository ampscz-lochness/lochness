"""
SupportedDataSourceTypes Model

This model defines the supported data source types for Lochness,
and defined their metadata dictionaries.
"""

from typing import Dict, Any, List
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
        data_source_type="webdav",
        data_source_metadata_dict={
            "keystore_name": "Name of the keystore entry containing the credentials",
            "endpoint_url": "WebDAV endpoint URL",
            "match_prefix": "Prefix to match files",
            "match_postfix": "Postfix to match files",
            "file_datastructure": "Data structure of the files (e.g. 'multi-subject')",
            "modality": "Modality of the data (e.g., 'cantab')",
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
