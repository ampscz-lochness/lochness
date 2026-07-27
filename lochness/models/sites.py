"""
Site Model
"""

from typing import Any, Dict

from pydantic import BaseModel

from lochness.helpers import db


class Site(BaseModel):
    """
    A Site represents a participant in a site.
    A Site is a collection of subjects.

    Attributes:
        site_id (str): Unique identifier for the site.
        site_name (str): Name of the site.
        project_id (str): Identifier for the project to which the site belongs.
        site_is_active (bool): Indicates if the site is active.
        site_metadata (Dict[str, Any]): Metadata associated with the site.
    """

    site_id: str
    site_name: str
    project_id: str
    site_is_active: bool = True
    site_metadata: Dict[str, Any]

    @staticmethod
    def init_db_table_query() -> str:
        """
        Returns the SQL query to create the database table for sites.
        """
        sql_query = """
            CREATE TABLE sites (
                site_id TEXT NOT NULL,
                project_id TEXT NOT NULL,
                site_name TEXT NOT NULL,
                site_is_active BOOLEAN DEFAULT TRUE NOT NULL,
                site_metadata JSONB NOT NULL,
                PRIMARY KEY (project_id, site_id),
                FOREIGN KEY (project_id) REFERENCES projects(project_id)
            );
        """

        return sql_query

    @staticmethod
    def drop_db_table_query() -> str:
        """
        Returns the SQL query to drop the database table for sites.
        """
        sql_query = """
            DROP TABLE IF EXISTS sites;
        """

        return sql_query

    def __str__(self) -> str:
        return f"[Site ID: {self.site_id} | Project ID: {self.project_id}]"

    def __repr__(self) -> str:
        return self.__str__()

    def to_sql_query(self) -> str:
        """
        Returns the SQL query to insert the site into the database.
        """
        site_metadata = db.sanitize_json(self.site_metadata)
        sql_query = f"""
            INSERT INTO sites (
                project_id, site_id, site_name, site_is_active, site_metadata
            ) VALUES (
                '{self.project_id}', '{self.site_id}', '{self.site_name}', {self.site_is_active}, '{site_metadata}'
            ) ON CONFLICT (project_id, site_id) DO UPDATE
            SET
                site_name = EXCLUDED.site_name,
                site_metadata = EXCLUDED.site_metadata;
        """

        return sql_query

    def delete_record_query(self) -> str:
        """Generate a query to delete a record from the table"""
        query = f"""DELETE FROM sites
        WHERE site_id = '{self.site_id}'
          AND project_id = '{self.project_id}';"""
        return query
