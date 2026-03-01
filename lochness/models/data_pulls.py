"""
DataPull Model
"""

from typing import Dict, Any, Optional
from pathlib import Path

from pydantic import BaseModel
import pandas as pd

from lochness.helpers import db, utils
from lochness.models.data_source import DataSource


class DataPull(BaseModel):
    """
    A Data Pull represents a download / data pull from
    a configured data source to a local file system.

    Attributes:
        subject_id (str): Subject associated with the data pull.
        data_source_name (str): Name of the data source.
        site_id (str): site associated with the data pull.
        project_id (str): Project associated with the data pull.
        file_path (str): Path to the file.
        pull_time_s (int): Time taken for the data pull in seconds.
        pull_metadata (Dict[str, Any]): Metadata associated with the data pull.
    """

    subject_id: Optional[str] = None
    data_source_name: str
    site_id: str
    project_id: str
    file_path: str
    file_md5: str
    pull_time_s: int
    pull_metadata: Dict[str, Any]

    @staticmethod
    def init_db_table_query() -> str:
        """
        Returns the SQL query to create the database table for data pulls.
        """
        sql_query = """
            CREATE TABLE IF NOT EXISTS data_pull (
                data_pull_id SERIAL PRIMARY KEY,
                subject_id TEXT,
                data_source_name TEXT NOT NULL,
                site_id TEXT NOT NULL,
                project_id TEXT NOT NULL,
                file_path TEXT NOT NULL,
                file_md5 TEXT NOT NULL,
                pull_time_s INTEGER NOT NULL,
                pull_timestamp TIMESTAMPTZ DEFAULT NOW(),
                pull_metadata JSONB NOT NULL,
                FOREIGN KEY (subject_id, site_id, project_id)
                    REFERENCES subjects (subject_id, site_id, project_id),
                FOREIGN KEY (data_source_name, site_id, project_id)
                    REFERENCES data_sources (data_source_name, site_id, project_id),
                FOREIGN KEY (file_path, file_md5)
                    REFERENCES files (file_path, file_md5)
            );
        """
        return sql_query

    @staticmethod
    def drop_db_table_query() -> str:
        """
        Returns the SQL query to drop the database table for data pulls.
        """
        sql_query = """
            DROP TABLE IF EXISTS data_pull CASCADE;
        """
        return sql_query

    def to_sql_query(self) -> str:
        """
        Returns the SQL query to insert the data pull into the database.
        """
        subject_id = db.sanitize_string(self.subject_id)
        data_source_name = db.sanitize_string(self.data_source_name)
        site_id = db.sanitize_string(self.site_id)
        project_id = db.sanitize_string(self.project_id)
        file_path = db.sanitize_string(self.file_path)
        file_md5 = db.sanitize_string(self.file_md5)
        pull_time_s = self.pull_time_s
        pull_metadata = db.sanitize_json(self.pull_metadata)

        sql_query = f"""
            INSERT INTO data_pull (subject_id, data_source_name, site_id, project_id,
                file_path, file_md5, pull_time_s, pull_metadata)
            VALUES ('{subject_id}', '{data_source_name}', '{site_id}', '{project_id}',
                '{file_path}', '{file_md5}', {pull_time_s}, '{pull_metadata}');
        """
        return sql_query

    def __str__(self) -> str:
        """
        Returns a user-friendly string representation of the DataPull object.
        """
        return (
            f"DataPull("
            f"subject_id={self.subject_id}, "
            f"data_source_name={self.data_source_name}, "
            f"site_id={self.site_id}, "
            f"project_id={self.project_id}, "
            f"file_path={self.file_path}, "
            f"file_md5={self.file_md5}, "
            f"pull_time_s={self.pull_time_s}, "
            f"pull_metadata={self.pull_metadata}"
            f")"
        )

    def __repr__(self) -> str:
        """
        Returns a detailed string representation of the DataPull object for debugging.
        """
        return self.__str__()

    def get_associated_data_source(self, config_file: Path) -> DataSource:
        """
        Returns the associated data source name for this data pull.

        Args:
            config_file (Path): Path to the Lochness configuration file.

        Returns:
            DataSource: The DataSource object associated with this data pull.
        """
        data_source = DataSource.get(
            data_source_name=self.data_source_name,
            site_id=self.site_id,
            project_id=self.project_id,
            config_file=config_file,
        )

        return data_source

    @staticmethod
    def get_most_recent_data_pull(
        config_file: Path, file_path: str, file_md5: str
    ) -> Optional["DataPull"]:
        """
        Returns the most recent data_pull record for the given
        file_path and file_md5.

        Args:
            file_path (str): Path to the file.
            file_md5 (str): MD5 hash of the file.
            config_file (str): Path to the database configuration file.

        Returns:
            DataPull: The most recent data_pull record.
        """
        sql_query = f"""
            SELECT * FROM data_pull
            WHERE file_path = '{file_path}'
              AND file_md5 = '{file_md5}'
            ORDER BY pull_timestamp DESC
            LIMIT 1;
        """

        sql_query = db.handle_null(sql_query)
        result_df = db.execute_sql(config_file, sql_query)

        if result_df.empty:
            return None

        row = result_df.iloc[0]

        return DataPull(
            subject_id=row["subject_id"],
            data_source_name=row["data_source_name"],
            site_id=row["site_id"],
            project_id=row["project_id"],
            file_path=row["file_path"],
            file_md5=row["file_md5"],
            pull_time_s=row["pull_time_s"],
            pull_metadata=row["pull_metadata"],
        )

    def delete_record_query(self) -> str:
        """Generate a query to delete a record from the table"""
        query = f"""DELETE FROM data_pull
        WHERE subject_id = '{self.subject_id}'
          AND data_source_name = '{self.data_source_name}'
          AND project_id = '{self.project_id}'
          AND site_id = '{self.site_id}'
          AND file_path = '{self.file_path}'
          AND file_md5 = '{self.file_md5}';"""
        return query

    @staticmethod
    def get_data_pulls_for_subject(
        config_file: Path,
        subject_id: str,
        site_id: str,
        project_id: str,
        data_source_name: str,
    ) -> pd.DataFrame:
        """
        Retrieve all data pulls for a specific subject and data source.

        Args:
            config_file (str): Path to the database configuration file.
            subject_id (str): Subject identifier.
            site_id (str): Site identifier.
            project_id (str): Project identifier.
            data_source_name (str): Data source name.

        Returns:
            pd.DataFrame: DataFrame containing all matching data pulls.
        """
        sql_query = f"""
            SELECT * FROM data_pull
            WHERE subject_id = '{subject_id}'
              AND site_id = '{site_id}'
              AND project_id = '{project_id}'
              AND data_source_name = '{data_source_name}'
            ORDER BY pull_timestamp DESC;
        """

        result_df = db.execute_sql(config_file, sql_query)
        result_df = utils.explode_col(result_df, "pull_metadata")

        return result_df
