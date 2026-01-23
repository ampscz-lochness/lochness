"""
Data Sinks refer to the destination systems or services where data is
pushed or stored after aquisition. This can include Object Stores,
File Systems, or any other storage solutions.
"""

from pathlib import Path
from typing import Any, Dict, List, Optional

from pydantic import BaseModel

from lochness.helpers import db


class DataSink(BaseModel):
    """
    A DataSink referes to a place where data is to be aggregated.

    Attributes:
        data_sink_name (str): Name of the data sink.
        data_sink_metadata (Dict[str, Any]): Metadata associated with the data sink.
    """

    data_sink_name: str
    is_active: bool = True
    site_id: str
    project_id: str
    data_sink_metadata: Dict[str, Any]

    @staticmethod
    def init_db_table_query() -> str:
        """
        Returns the SQL query to create the database table for data sinks.
        """
        sql_query = """
            CREATE TABLE IF NOT EXISTS data_sinks (
                data_sink_id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                data_sink_name TEXT,
                data_sink_is_active BOOLEAN DEFAULT TRUE,
                site_id TEXT NOT NULL,
                project_id TEXT NOT NULL,
                data_sink_metadata JSONB NOT NULL,
                FOREIGN KEY (site_id, project_id)
                    REFERENCES sites (site_id, project_id),
                UNIQUE (data_sink_name, site_id, project_id)
            );
        """

        return sql_query

    @staticmethod
    def drop_db_table_query() -> str:
        """
        Returns the SQL query to drop the database table for data sinks.
        """
        sql_query = """
            DROP TABLE IF EXISTS data_sinks;
        """

        return sql_query

    def __str__(self) -> str:
        return (
            f"[Data Sink: {self.data_sink_name} | site ID: {self.site_id} | "
            f"Project ID: {self.project_id}]"
        )

    def __repr__(self) -> str:
        return self.__str__()

    def to_sql_query(self) -> str:
        """
        Returns the SQL query to insert the data sink into the database.
        """

        metadata_str = db.sanitize_json(self.data_sink_metadata)
        data_sink_name = db.sanitize_string(self.data_sink_name)

        sql_query = f"""
            INSERT INTO data_sinks (
                data_sink_name, data_sink_is_active,
                site_id, project_id, data_sink_metadata
            ) VALUES (
                '{data_sink_name}', {self.is_active},
                '{self.site_id}', '{self.project_id}', '{metadata_str}'
            )
            ON CONFLICT (data_sink_name, site_id, project_id)
            DO UPDATE SET data_sink_metadata = EXCLUDED.data_sink_metadata;
        """
        return sql_query

    @staticmethod
    def get_all_data_sinks(
        config_file: Path, active_only: bool = False
    ) -> List["DataSink"]:
        """
        Retrieves all data sinks from the database.

        Args:
            config_file (Path): Path to the configuration file.
            active_only (bool): If True, only return active data sinks (based on metadata).

        Returns:
            List[DataSink]: A list of DataSink objects.
        """
        query = """
            SELECT data_sink_name, data_sink_is_active, site_id, project_id, data_sink_metadata
            FROM data_sinks;
        """
        data_sinks_df = db.execute_sql(config_file, query)

        data_sinks: List[DataSink] = []
        for _, row in data_sinks_df.iterrows():
            data_sink = DataSink(
                data_sink_name=row["data_sink_name"],
                is_active=row["data_sink_is_active"],
                site_id=row["site_id"],
                project_id=row["project_id"],
                data_sink_metadata=row["data_sink_metadata"],
            )
            # If active_only is True, check for 'is_active': True in metadata
            if active_only and not data_sink.is_active:
                continue
            data_sinks.append(data_sink)
        return data_sinks

    @staticmethod
    def get_matching_data_sink(
        config_file: Path,
        site_id: str,
        project_id: str,
        active_only: bool = False,
        data_sink_name: Optional[str] = None,
    ) -> "Optional[DataSink]":
        """
        Retrieves the matching data sink

        Args:
            config_file (Path): Path to the configuration file.
            data_sink_name (str): Name of the data sink.
            site_id (str): Site ID.
            project_id (str): Project ID.
            active_only (bool): If True, only return active data sinks (based on metadata).

        Returns:
            List[DataSink]: A list of DataSink objects.
        """

        if data_sink_name:
            query = f"""
            SELECT data_sink_name, data_sink_is_active,
                site_id, project_id, data_sink_metadata
            FROM data_sinks
            WHERE data_sink_name = '{data_sink_name}'
                AND site_id = '{site_id}'
                AND project_id = '{project_id}'
            LIMIT 1;
            """
        else:
            query = f"""
            SELECT data_sink_name, data_sink_is_active,
                site_id, project_id, data_sink_metadata
            FROM data_sinks
            WHERE site_id = '{site_id}' AND project_id = '{project_id}'
            LIMIT 1;
            """
        data_sinks_df = db.execute_sql(config_file, query)
        row = data_sinks_df.iloc[0]
        data_sink = DataSink(
            data_sink_name=row["data_sink_name"],
            is_active=row["data_sink_is_active"],
            site_id=row["site_id"],
            project_id=row["project_id"],
            data_sink_metadata=row["data_sink_metadata"],
        )

        if active_only and not data_sink.is_active:
            return None

        return data_sink

    def get_data_sink_id(self, config_file: Path) -> Optional[int]:
        """
        Get the data sink ID for the current data sink.

        Args:
            config_file (Path): Path to the configuration file.
        Returns:
            str: The data sink ID.
        """

        query = f"""
            SELECT data_sink_id
            FROM data_sinks
            WHERE
                data_sink_name = '{self.data_sink_name}'
                AND site_id = '{self.site_id}'
                AND project_id = '{self.project_id}'
            LIMIT 1;
            """

        data_sink_id = db.fetch_record(config_file=config_file, query=query)

        if data_sink_id is None:
            return None

        return int(data_sink_id)

    @staticmethod
    def get_data_sink_by_id(
        config_file: Path, data_sink_id: int
    ) -> "Optional[DataSink]":
        """
        Retrieves a DataSink by its ID.

        Args:
            config_file (Path): Path to the configuration file.
            data_sink_id (int): The data sink ID.

        Returns:
            Optional[DataSink]: The DataSink object if found, None otherwise.
        """
        query = f"""
            SELECT data_sink_name, data_sink_is_active,
                site_id, project_id, data_sink_metadata
            FROM data_sinks
            WHERE data_sink_id = {data_sink_id}
            LIMIT 1;
        """
        data_sinks_df = db.execute_sql(config_file, query)

        if data_sinks_df.empty:
            return None

        row = data_sinks_df.iloc[0]
        data_sink = DataSink(
            data_sink_name=row["data_sink_name"],
            is_active=row["data_sink_is_active"],
            site_id=row["site_id"],
            project_id=row["project_id"],
            data_sink_metadata=row["data_sink_metadata"],
        )

        return data_sink

    def delete_record_query(self) -> str:
        """
        Generate a query to delete a record from the table
        """
        query = f"""
        DELETE FROM data_sinks
        WHERE
            data_sink_name = '{self.data_sink_name}'
            AND site_id = '{self.site_id}'
            AND project_id = '{self.project_id}';
        """
        return query
