#!/usr/bin/env python
"""
File Model
"""

from pathlib import Path
from datetime import datetime
from typing import Optional, List, Dict, Any

from lochness.helpers import db, utils
from lochness.helpers import hash as hash_helper
from lochness.models.data_pulls import DataPull


class File:
    """
    Represents a file.

    Attributes:
        file_path (Path): The path to the file.
    """

    def __init__(self, file_path: Path, with_hash: bool = True):
        """
        Initialize a File object.

        Args:
            file_path (Path): The path to the file.
        """
        self.file_path = file_path

        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        self.file_name = file_path.name
        self.file_type = file_path.suffix
        if self.file_type == ".lock":
            # Use previous suffix for lock files
            self.file_type = file_path.suffixes[-2]

        self.file_size_mb = file_path.stat().st_size / 1024 / 1024
        self.m_time = datetime.fromtimestamp(file_path.stat().st_mtime)
        if with_hash:
            self.md5 = hash_helper.compute_fingerprint(file_path=file_path)
        else:
            self.md5 = None

        self.file_metadata: Dict[str, Any] = {
            "available_at": [f"hn:{utils.get_hostname()}"]
        }
        self.internal_metadata: Dict[str, Any] = {}

    @staticmethod
    def new(
        file_path: Path,
        file_size_mb: float,
        m_time: datetime,
        md5: Optional[str] = None,
        file_metadata: Optional[Dict[str, Any]] = None,
    ) -> "File":
        """
        Create a new File object from given parameters.

        Args:
            file_name (str): The name of the file.
            file_type (str): The type of the file.
            file_size_mb (float): The size of the file in MB.
            file_path (Path): The path to the file.
            m_time (datetime): The modification time of the file.
            md5 (Optional[str]): The MD5 hash of the file.
            file_metadata: Optional[Dict[str, Any]] = None,

        Returns:
            File: A new File object.
        """

        file_obj = object.__new__(File)
        file_obj.file_name = file_path.name

        file_type = file_path.suffix
        if file_type == ".lock":
            # Use previous suffix for lock files
            file_type = file_path.suffixes[-2]
        file_obj.file_type = file_type

        file_obj.file_size_mb = file_size_mb
        file_obj.file_path = file_path
        file_obj.m_time = m_time
        file_obj.md5 = md5
        file_obj.internal_metadata = {}
        file_obj.file_metadata = file_metadata if file_metadata else {}

        return file_obj

    def __str__(self):
        """
        Return a string representation of the File object.
        """
        return f"File({self.file_name}, {self.file_type}, {self.file_size_mb}, \
            {self.file_path}, {self.m_time}, {self.md5})"

    def __repr__(self):
        """
        Return a string representation of the File object.
        """
        return self.__str__()

    @staticmethod
    def init_db_table_query() -> str:
        """
        Return the SQL query to create the 'files' table.
        """
        sql_query = """
        CREATE TABLE IF NOT EXISTS files (
            file_name TEXT NOT NULL,
            file_type TEXT NOT NULL,
            file_size_mb FLOAT NOT NULL,
            file_path TEXT NOT NULL,
            file_m_time TIMESTAMP NOT NULL,
            file_md5 TEXT NOT NULL,
            file_metadata JSONB NOT NULL,
            PRIMARY KEY (file_path, file_md5)
        );
        """

        return sql_query

    @staticmethod
    def get_most_recent_file_obj(
        config_file: Path, file_path: Path
    ) -> Optional["File"]:
        """
        Return the most recent record that matches the given file_path.

        Args:
            config_file (Path): The database configuration file.
            file_path (Path): The path to the file.

        Returns:
            File: The most recent File object.
        """
        f_path = db.sanitize_string(str(file_path))
        sql_query = f"""
        SELECT DISTINCT files.*,
            data_sources.data_source_metadata->>'modality' as modality
        FROM files
        JOIN data_pull on data_pull.file_path = files.file_path AND
            data_pull.file_md5 = files.file_md5
        JOIN data_sources on
            data_sources.data_source_name = data_pull.data_source_name
            AND data_sources.site_id = data_pull.site_id
            AND data_sources.project_id = data_pull.project_id
        WHERE files.file_path = '{f_path}'
        ORDER BY files.file_m_time DESC
        LIMIT 1;
        """

        sql_query = db.handle_null(sql_query)
        result_df = db.execute_sql(config_file, sql_query)

        if result_df.empty:
            return None

        row = result_df.iloc[0]
        # file_obj = object.__new__(File)
        # file_obj.file_path = Path(row["file_path"])
        # file_obj.file_name = Path(row["file_path"]).name
        # file_obj.file_type = Path(row["file_path"]).suffix
        # file_obj.md5 = row["file_md5"]
        # file_obj.m_time = row["file_m_time"]
        # file_obj.file_size_mb = row["file_size_mb"]
        # file_obj.file_type = row["file_type"]
        # file_obj.file_name = row["file_name"]

        file_obj = File.new(
            file_path=Path(row["file_path"]),
            file_size_mb=row["file_size_mb"],
            m_time=row["file_m_time"],
            md5=row["file_md5"],
            file_metadata=row["file_metadata"],
        )

        file_obj.internal_metadata = {"modality": row["modality"]}

        return file_obj

    @staticmethod
    def get_recent_data_pull(
        file_path: Path,
        config_file: Path
    ) -> Optional[DataPull]:
        """
        Return the most recent data pull for the given file path.

        Args:
            file_path (Path): The path to the file.
            config_file (Path): The database configuration file.

        Returns:
            DataPull: The most recent DataPull object.
        """
        f_path = db.sanitize_string(str(file_path))
        sql_query = f"""
        SELECT *
        FROM data_pull
        WHERE file_path = '{f_path}'
        ORDER BY pull_time_s DESC
        LIMIT 1;
        """

        sql_query = db.handle_null(sql_query)
        result_df = db.execute_sql(config_file, sql_query)

        if result_df.empty:
            return None

        row = result_df.iloc[0]
        data_pull = DataPull(
            file_path=row["file_path"],
            file_md5=row["file_md5"],
            pull_time_s=row["pull_time_s"],
            pull_metadata=row["pull_metadata"],
            data_source_name=row["data_source_name"],
            subject_id=row["subject_id"],
            site_id=row["site_id"],
            project_id=row["project_id"],
        )

        return data_pull

    @staticmethod
    def drop_db_table_query() -> str:
        """
        Return the SQL query to drop the 'files' table if it exists.
        """
        sql_query = """
        DROP TABLE IF EXISTS files CASCADE;
        """

        return sql_query

    def to_sql_query(self) -> str:
        """
        WARNING: Internal Use Only. Use `to_sql_queries_with_availability_update` instead.

        Return the SQL query to insert the File object into the 'files' table.
        """
        f_name = db.sanitize_string(self.file_name)
        f_path = db.sanitize_string(str(self.file_path))

        if self.md5 is None:
            hash_val = "NULL"
        else:
            hash_val = self.md5

        file_metadata_json = db.sanitize_json(self.file_metadata)

        sql_query = f"""
        INSERT INTO files (file_name, file_type, file_size_mb,
            file_path, file_m_time, file_md5, file_metadata)
        VALUES ('{f_name}', '{self.file_type}', '{self.file_size_mb}',
            '{f_path}', '{self.m_time}', '{hash_val}', '{file_metadata_json}')
        ON CONFLICT (file_path, file_md5) DO UPDATE SET
            file_name = excluded.file_name,
            file_type = excluded.file_type,
            file_size_mb = excluded.file_size_mb,
            file_m_time = excluded.file_m_time,
            file_metadata = excluded.file_metadata;
        """

        sql_query = db.handle_null(sql_query)

        return sql_query

    def to_sql_queries_with_availability_update(self) -> List[str]:
        """
        Return SQL queries to insert the File object and update old versions.

        When a new file is written to the same path, the old file at that path
        is no longer available locally. This method generates:
        1. A query to remove the current hostname from old versions at same path
        2. A query to insert/update the current file record

        Returns:
            List[str]: List of SQL queries to execute in order.
        """
        queries: List[str] = []
        hostname_location = f"hn:{utils.get_hostname()}"

        if self.md5 is not None:
            remove_old_query = File.remove_availability_query(
                file_path=self.file_path,
                current_md5=self.md5,
                location=hostname_location,
            )
            queries.append(remove_old_query)

        queries.append(self.to_sql_query())

        return queries

    @staticmethod
    def get_files_to_push(
        config_file: Path,
        project_id: str,
        site_id: str,
        data_sink_id: int,
    ) -> List["File"]:
        """
        Return the most recent version of files to push for a given project and site.
        Only returns the latest file (by file_m_time) for each unique file_path
        that has not yet been pushed to the specified data sink.
        """
        query = f"""
        SELECT files.*
        FROM (
            SELECT files.*,
                ROW_NUMBER() OVER (
                    PARTITION BY files.file_path
                    ORDER BY files.file_m_time DESC
                ) as rn
            FROM files
            LEFT JOIN data_pull ON (
                data_pull.file_path = files.file_path AND
                data_pull.file_md5 = files.file_md5
            )
            WHERE data_pull.project_id = '{project_id}'
                AND data_pull.site_id = '{site_id}'
        ) files
        LEFT JOIN data_push ON (
            data_push.file_path = files.file_path AND
            data_push.file_md5 = files.file_md5 AND
            data_push.data_sink_id = {data_sink_id}
        )
        WHERE files.rn = 1
            AND data_push.data_sink_id IS NULL;
        """

        db_df = db.execute_sql(
            config_file=config_file,
            query=query,
        )

        if db_df.empty:
            return []

        files: List[File] = []
        for _, row in db_df.iterrows():
            file_obj = object.__new__(File)
            file_obj.file_path = Path(row["file_path"])
            file_obj.file_name = row["file_name"]
            file_obj.file_type = row["file_type"]
            file_obj.file_size_mb = row["file_size_mb"]
            file_obj.m_time = row["file_m_time"]
            file_obj.md5 = row["file_md5"]
            file_obj.file_metadata = (
                row["file_metadata"] if row["file_metadata"] else {}
            )
            file_obj.internal_metadata = {}

            files.append(file_obj)

        return files

    @staticmethod
    def remove_availability_query(
        file_path: Path,
        current_md5: str,
        location: str,
    ) -> str:
        """
        Generate SQL query to remove a location from all previous versions of a file.

        When a file at the same path is overwritten (locally or in a data sink),
        the previous version is no longer available at that location. This method
        generates a query to update the `available_at` metadata for all files at
        the same path with a different MD5 hash.

        Args:
            file_path (Path): The file path to match.
            current_md5 (str): The MD5 of the current/new file (will be excluded).
            location (str): Location identifier to remove (e.g., 'ds:123' for data sink,
                'hn:hostname' for local copy)

        Returns:
            str: SQL query to update the old files' metadata.
        """
        f_path = db.sanitize_string(str(file_path))
        current_md5_sanitized = db.sanitize_string(current_md5)
        location_sanitized = db.sanitize_string(location)

        # Use PostgreSQL's jsonb operators to remove the location from available_at array
        # This handles all files at the same path with different MD5
        query = f"""
        UPDATE files
        SET file_metadata = jsonb_set(
            file_metadata,
            '{{available_at}}',
            (
                SELECT COALESCE(
                    jsonb_agg(elem),
                    '[]'::jsonb
                )
                FROM jsonb_array_elements(
                    COALESCE(file_metadata->'available_at', '[]'::jsonb)
                ) AS elem
                WHERE elem::text != '"{location_sanitized}"'
            )
        )
        WHERE file_path = '{f_path}'
            AND file_md5 != '{current_md5_sanitized}'
            AND file_metadata->'available_at' @> '["{location_sanitized}"]'::jsonb;
        """

        return query

    def delete_record_query(self) -> str:
        """Generate a query to delete a record from the table"""
        query = f"""
        DELETE
        FROM files
        WHERE file_path = '{self.file_path}'
            AND file_md5 = '{self.md5}';
        """
        return query

    def add_available_at_query(self, location: str) -> str:
        """
        Generate SQL query to add a location to the file's available_at metadata.
        Ensures no repetition by maintaining a set of locations.

        Args:
            location (str): Location identifier (e.g., 'ds:123' for data sink,
                'hn:hostname' for local copy)

        Returns:
            str: SQL query to update the file's metadata, or empty string if
                location already exists
        """
        # Get current available_at or initialize as list
        available_at = self.file_metadata.get("available_at", [])
        if isinstance(available_at, str):
            available_at = [available_at]
        elif not isinstance(available_at, list):
            available_at = []

        # Add location if not already present
        if location not in available_at:
            available_at.append(location)
            self.file_metadata["available_at"] = available_at

            # Generate update query
            file_metadata_json = db.sanitize_json(self.file_metadata)
            f_path = db.sanitize_string(str(self.file_path))
            update_query = f"""
            UPDATE files
            SET file_metadata = '{file_metadata_json}'
            WHERE file_path = '{f_path}' AND file_md5 = '{self.md5}';
            """
            return update_query
        return ""
