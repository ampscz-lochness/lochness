"""
Data Source Model
"""

import logging
from pathlib import Path
from typing import Any, Dict

from pydantic import BaseModel

from lochness.helpers import db

logger = logging.getLogger(__name__)


class DataSource(BaseModel):
    """
    A data source is any external source of data, that Lochnes
    can connect to and pull data from.
    """

    data_source_name: str
    is_active: bool
    site_id: str
    project_id: str
    data_source_type: str
    data_source_metadata: Dict[str, Any]

    @staticmethod
    def get(
        data_source_name: str,
        site_id: str,
        project_id: str,
        config_file: Path
    ) -> "DataSource":
        """
        Retrieve a DataSource object from the database.

        Args:
            data_source_name (str): Name of the data source.
            site_id (str): Site ID associated with the data source.
            project_id (str): Project ID associated with the data source.

        Returns:
            DataSource: The retrieved DataSource object.
        """
        query = f"""
            SELECT * FROM data_sources
            WHERE data_source_name = '{data_source_name}'
              AND site_id = '{site_id}'
              AND project_id = '{project_id}';
        """
        db_df = db.execute_sql(config_file=config_file, query=query)
        if db_df.empty:
            raise ValueError(f"Data source {data_source_name} not found.")

        data_source_name = db_df['data_source_name'][0]
        site_id = db_df['site_id'][0]
        project_id = db_df['project_id'][0]
        is_active = db_df['data_source_is_active'][0]
        data_source_type = db_df['data_source_type'][0]
        data_source_metadata = db_df['data_source_metadata'][0]

        data_source = DataSource(
            data_source_name=data_source_name,
            site_id=site_id,
            project_id=project_id,
            is_active=is_active,
            data_source_type=data_source_type,
            data_source_metadata=data_source_metadata
        )

        return data_source

    @staticmethod
    def init_db_table_query() -> str:
        """
        Returns the SQL query to create the database table for data sources.
        """
        sql_query = """
            CREATE TABLE data_sources (
                data_source_name TEXT NOT NULL,
                data_source_is_active BOOLEAN DEFAULT TRUE,
                site_id TEXT NOT NULL,
                project_id TEXT NOT NULL,
                data_source_type TEXT REFERENCES supported_data_source_types(data_source_type),
                data_source_metadata JSONB NOT NULL,
                PRIMARY KEY (data_source_name, site_id, project_id),
                FOREIGN KEY (site_id, project_id) REFERENCES sites(site_id, project_id)
            );
        """

        return sql_query

    @staticmethod
    def drop_db_table_query() -> str:
        """
        Returns the SQL query to drop the database table for data sources.
        """
        sql_query = """
            DROP TABLE IF EXISTS data_sources;
        """

        return sql_query

    def __str__(self) -> str:
        return (
            f"[Data Source: {self.data_source_name} {self.data_source_type} | "
            f"site ID: {self.site_id} | Project ID: {self.project_id}]"
        )

    def __repr__(self) -> str:
        return self.__str__()

    def to_sql_query(self) -> str:
        """
        Returns the SQL query to insert the data source into the database.
        """
        data_source_metadata = db.sanitize_json(self.data_source_metadata)
        sql_query = f"""
            INSERT INTO data_sources (
                data_source_name, data_source_is_active, site_id,
                project_id, data_source_type, data_source_metadata
            ) VALUES (
                '{self.data_source_name}', {self.is_active}, '{self.site_id}',
                '{self.project_id}', '{self.data_source_type}', '{data_source_metadata}'
            ) ON CONFLICT (data_source_name, site_id, project_id)
                DO UPDATE SET
                data_source_type = EXCLUDED.data_source_type,
                data_source_metadata = EXCLUDED.data_source_metadata;
        """

        return sql_query

    def delete_record_query(self) -> str:
        """Generate a query to delete a record from the table"""
        query = f"""DELETE FROM data_sources
        WHERE data_source_name = '{self.data_source_name}'
          AND project_id = '{self.project_id}'
          AND site_id = '{self.site_id}';"""
        return query
