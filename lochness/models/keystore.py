"""
KeyStore class for managing API keys and secrets.
"""

from pathlib import Path
from typing import Any, Dict, List, Optional

from pydantic import BaseModel

from lochness.helpers import config, db


class KeyStore(BaseModel):
    """
    KeyStore class for managing API keys and secrets.
    """

    key_name: str
    key_value: str
    key_type: str
    project_id: str
    key_metadata: Dict[str, Any] = {}

    @staticmethod
    def init_db_table_query() -> List[str]:
        """
        Returns the SQL query to create the database table for keys.
        """
        enable_extension_query = """
        CREATE EXTENSION IF NOT EXISTS pgcrypto;
        """
        sql_query = """
        CREATE TABLE IF NOT EXISTS key_store (
            key_name TEXT NOT NULL,
            project_id TEXT NOT NULL,
            key_value BYTEA NOT NULL,
            key_type TEXT NOT NULL,
            key_metadata JSONB,
            PRIMARY KEY (key_name, project_id),
            FOREIGN KEY (project_id) REFERENCES projects(project_id)
        );
        """
        return [enable_extension_query, sql_query]

    @staticmethod
    def drop_db_table_query() -> str:
        """
        Returns the SQL query to drop the database table for keys.
        """
        sql_query = """
            DROP TABLE IF EXISTS key_store;
        """
        return sql_query

    def to_sql_query(self, encryption_passphrase: str) -> str:
        """
        Converts the KeyStore instance to a SQL insert statement.
        """
        project_id = db.sanitize_string(self.project_id)
        key_name = db.sanitize_string(self.key_name)
        key_value = db.sanitize_string(self.key_value)
        key_type = db.sanitize_string(self.key_type)
        key_metadata = db.sanitize_json(self.key_metadata)

        sql = f"""
        INSERT INTO key_store (
            key_name,
            project_id,
            key_value,
            key_type,
            key_metadata
        ) VALUES (
            '{key_name}',
            '{project_id}',
            pgp_sym_encrypt('{key_value}', '{encryption_passphrase}'),
            '{key_type}',
            '{key_metadata}'
        ) ON CONFLICT (key_name, project_id)
        DO UPDATE SET key_value = EXCLUDED.key_value,
                      key_type = EXCLUDED.key_type,
                      key_metadata = EXCLUDED.key_metadata;
        """
        return sql

    @staticmethod
    def retrieve_key_query(
        key_name: str, project_id: str, encryption_passphrase: str
    ) -> str:
        """
        Returns the SQL query to retrieve a key from the database for a specific project.
        """
        key_name = db.sanitize_string(key_name)
        project_id = db.sanitize_string(project_id)
        sql = f"""
            SELECT pgp_sym_decrypt(key_value, '{encryption_passphrase}') AS key_value
            FROM key_store
            WHERE key_name = '{key_name}' AND project_id = '{project_id}';
        """
        return sql

    @staticmethod
    def retrieve_key_metadata(key_name: str, project_id: str) -> str:
        """
        Returns the SQL query to retrieve a key from the database for a specific project.
        """
        sql = f"""
            SELECT key_metadata
            FROM key_store
            WHERE key_name = '{key_name}'
              AND project_id = '{project_id}';
        """
        return sql

    @staticmethod
    def retrieve_keystore(
        key_name: str,
        project_id: str,
        config_file: Path,
    ) -> Optional["KeyStore"]:
        """
        Retrieves a KeyStore entry by its name and project ID.
        """

        encryption_passphrase: str = config.get_encryption_passphrase(config_file)

        query = f"""
            SELECT
                pgp_sym_decrypt(key_value, '{encryption_passphrase}') AS key_value,
                key_type,
                key_metadata
            FROM key_store
            WHERE key_name = '{db.sanitize_string(key_name)}'
                AND project_id = '{db.sanitize_string(project_id)}';
        """

        result_df = db.execute_sql(
            config_file=config_file,
            query=query,
        )

        if result_df is not None and not result_df.empty:
            key_value_raw: str = result_df["key_value"].values[0]  # type: ignore
            key_type_raw: str = result_df["key_type"].values[0]  # type: ignore
            key_metadata_raw: Dict[str, Any] = result_df["key_metadata"].values[0]  # type: ignore

            return KeyStore(
                key_name=key_name,
                key_value=key_value_raw,
                key_type=key_type_raw,
                project_id=project_id,
                key_metadata=key_metadata_raw,
            )

        return None

    @staticmethod
    def get_by_name_and_project(
        config_file: Path, key_name: str, project_id: str, encryption_passphrase: str
    ) -> Optional["KeyStore"]:
        """
        Retrieves a KeyStore entry by its name and project ID.
        """
        query = KeyStore.retrieve_key_query(key_name, project_id, encryption_passphrase)
        key_value_raw = db.fetch_record(config_file, query)
        if key_value_raw:

            return KeyStore(
                key_name=key_name,
                key_value=key_value_raw,
                key_type="",
                project_id=project_id,
            )
        else:
            return None

    def delete_record_query(self) -> str:
        """
        Generate a query to delete a record from the table
        """

        key_name = db.sanitize_string(self.key_name)
        project_id = db.sanitize_string(self.project_id)

        query = f"""
        DELETE FROM key_store
        WHERE key_name = '{key_name}'
            AND project_id = '{project_id}';
        """
        return query
