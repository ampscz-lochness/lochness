"""
Projects are a collection of Studies
"""

from typing import Any, Dict

from pydantic import BaseModel

from lochness.helpers import db


class Project(BaseModel):
    """
    A project is a collection of studies

    Attributes:
        project_id (str): Unique identifier for the project.
        project_name (str): Name of the project.
        project_metadata (Dict[str, Any]): Metadata associated with the project.
    """

    project_id: str
    project_name: str
    project_is_active: bool = True
    project_metadata: Dict[str, Any]

    @staticmethod
    def init_db_table_query() -> str:
        """
        Returns the SQL query to create the database table for projects.
        """
        sql_query = """
            CREATE TABLE projects (
                project_id TEXT PRIMARY KEY,
                project_name TEXT NOT NULL,
                project_is_active BOOLEAN DEFAULT TRUE NOT NULL,
                project_metadata JSONB NOT NULL
            );
        """

        return sql_query

    @staticmethod
    def drop_db_table_query() -> str:
        """
        Returns the SQL query to drop the database table for projects.
        """
        sql_query = """
            DROP TABLE IF EXISTS projects;
        """

        return sql_query

    def __str__(self) -> str:
        """
        Returns a string representation of the project.
        """
        return f"[Project ID: {self.project_id}]"

    def __repr__(self) -> str:
        """
        Returns a string representation of the project.
        """
        return self.__str__()

    def to_sql_query(self) -> str:
        """
        Returns the SQL query to insert the project into the database.
        """
        metadata = db.sanitize_json(self.project_metadata)
        sql_query = f"""
            INSERT INTO projects (
                project_id, project_name, project_is_active, project_metadata
            ) VALUES (
                '{self.project_id}', '{self.project_name}', {self.project_is_active}, '{metadata}'
            )
            ON CONFLICT (project_id) DO UPDATE SET
                project_name = EXCLUDED.project_name,
                project_metadata = EXCLUDED.project_metadata;
        """

        return sql_query

    def delete_record_query(self) -> str:
        """Generate a query to delete a record from the table"""
        query = f"""DELETE FROM projects
        WHERE project_id = '{self.project_id}';"""
        return query
