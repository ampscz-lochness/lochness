"""
Data Source Model
"""

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

from pydantic import BaseModel

from lochness.helpers import db
from lochness.helpers.db import get_db_connection


class RedcapDataSourceMetadata(BaseModel):
    """
    Metadata for a REDCap data source.
    """

    keystore_name: str
    endpoint_url: str
    optional_variables_dictionary: List[Dict[str, str]]
    main_redcap: bool = False
    subject_id_variable: Optional[str]
    subject_id_variable_as_the_pk: bool = True
    messy_subject_id: bool = False
    dictionary: Optional[Dict] = None


class RedcapDataSource(BaseModel):
    """
    A REDCap data source is a specific type of data source that Lochness can connect to
    and pull data from.
    """

    data_source_name: str
    is_active: bool
    site_id: str
    project_id: str
    data_source_type: str
    data_source_metadata: RedcapDataSourceMetadata

    @staticmethod
    def get_all_redcap_data_sources(
        config_file: Path,
        active_only: bool = True,
    ) -> List["RedcapDataSource"]:
        """
        Get all active REDCap data sources.

        Returns:
            List[RedcapDataSource]: A list of active REDCap data sources.
        """
        sql_query = """
            SELECT *
            FROM data_sources
            WHERE data_source_type = 'redcap'
        """

        if active_only:
            sql_query += " AND data_source_is_active = TRUE"

        df = db.execute_sql(
            config_file=config_file,
            query=sql_query,
        )

        def convert_to_redcap_data_source(row: Dict[str, Any]) -> "RedcapDataSource":
            """
            Convert a row from the data_sources table to a RedcapDataSource object.

            Args:
                row (Dict[str, Any]): A dictionary representing a row from the data_sources table.

            Returns:
                RedcapDataSource: A RedcapDataSource object.
            """
            # Handle missing optional_variables_dictionary with default empty list
            optional_variables = (
                row["data_source_metadata"]
                .get(
                    "optional_variables_dictionary",
                    []
                )
            )

            redcap_data_source = RedcapDataSource(
                data_source_name=row["data_source_name"],
                is_active=row["data_source_is_active"],
                site_id=row["site_id"],
                project_id=row["project_id"],
                data_source_type=row["data_source_type"],
                data_source_metadata=RedcapDataSourceMetadata(
                    keystore_name=row["data_source_metadata"]["keystore_name"],
                    endpoint_url=row["data_source_metadata"]["endpoint_url"],
                    subject_id_variable=row["data_source_metadata"][
                        "subject_id_variable"
                    ],
                    optional_variables_dictionary=optional_variables,
                    main_redcap=row["data_source_metadata"]["main_redcap"],
                ),
            )
            return redcap_data_source

        redcap_data_sources: List[RedcapDataSource] = []

        for _, row in df.iterrows():  # type: ignore
            redcap_data_source = convert_to_redcap_data_source(row.to_dict())  # type: ignore
            redcap_data_sources.append(redcap_data_source)

        return redcap_data_sources

    @staticmethod
    def update_data_source_metadata_dictionary(
        config_file: Path,
        project_id: str,
        site_id: str,
        dictionary: Dict[str, Any],
    ) -> None:
        """
        Update the REDCap metadata history for a specific project and site.

        Args:
            config_file (Path): The path to the configuration file.
            project_id (str): The project ID.
            site_id (str): The site ID.
            dictionary (Dict): The REDCap dictionary to update.
        """
        path = "{dictionary}"
        value_json = json.dumps(dictionary)

        value_json = value_json.replace("'", "''")

        sql_query = f"""
        UPDATE data_sources
        SET data_source_metadata = jsonb_set(
            data_source_metadata,
            '{path}',
            '{value_json}'::jsonb
        )
        WHERE project_id = '{project_id}'
          AND site_id = '{site_id}'
          AND data_source_type = 'redcap'
        """

        engine = get_db_connection(config_file=config_file)
        with engine.begin() as conn:
            cur = conn.connection.cursor()
            cur.execute(sql_query)
