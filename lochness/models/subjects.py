"""
Subject Model
"""

import json
from pathlib import Path
from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel

from lochness.helpers import db


class Subject(BaseModel):
    """
    A subject is a unique entity in a site, such as a patient or a sample.
    It is identified by a unique subject ID within the context of a site.

    Attributes:
        subject_id (str): Unique identifier for the subject.
        site_id (str): Unique identifier for the site.
        project_id (str): Unique identifier for the project.
        subject_metadata (Dict[str, Any]): Metadata associated with the subject.
    """

    subject_id: str
    site_id: str
    project_id: str
    subject_metadata: Dict[str, Any]

    @staticmethod
    def init_db_table_query() -> str:
        """
        Returns the SQL query to create the database table for subjects.
        """
        sql_query = """
            CREATE TABLE subjects (
                subject_id TEXT NOT NULL,
                site_id TEXT NOT NULL,
                project_id TEXT NOT NULL,
                subject_metadata JSONB NOT NULL,
                PRIMARY KEY (subject_id, site_id, project_id),
                FOREIGN KEY (site_id, project_id) REFERENCES sites(site_id, project_id)
            );
        """

        return sql_query

    @staticmethod
    def drop_db_table_query() -> str:
        """
        Returns the SQL query to drop the database table for subjects.
        """
        sql_query = """
            DROP TABLE IF EXISTS subjects;
        """

        return sql_query

    def __str__(self) -> str:
        """
        Returns a string representation of the subject.
        """
        return (
            f"[Subject: {self.subject_id} | site ID: {self.site_id} | "
            f"Project ID: {self.project_id}]"
        )

    def __repr__(self) -> str:
        """
        Returns a string representation of the subject.
        """
        return self.__str__()

    def to_sql_query(self) -> str:
        """
        Returns the SQL query to insert the subject into the database.

        If a conflict occurs, merges the subject_metadata JSONB:
        - Existing keys are preserved unless overwritten by new values.
        - New keys are added.
        """
        subject_metadata = db.sanitize_json(self.subject_metadata)
        sql_query = f"""
            INSERT INTO subjects (
                subject_id, site_id, project_id,
                subject_metadata
            ) VALUES (
                '{self.subject_id}', '{self.site_id}', '{self.project_id}',
                '{subject_metadata}'
            ) ON CONFLICT (subject_id, site_id, project_id) DO UPDATE
            SET subject_metadata = subjects.subject_metadata || EXCLUDED.subject_metadata
            WHERE subjects.subject_metadata IS DISTINCT FROM subjects.subject_metadata || EXCLUDED.subject_metadata;
        """
        return sql_query

    @staticmethod
    def get(
        project_id: str, site_id: str, subject_id: str, config_file: Path
    ) -> "Subject":
        """
        Retrieves a subject by its ID from the database.

        Args:
            project_id (str): The project ID.
            site_id (str): The site ID.
            subject_id (str): The subject ID.
            config_file (Path): Path to the configuration file.

        Returns:
            Subject: The retrieved Subject object.
        """
        query = f"""
        SELECT *
        FROM subjects
        WHERE project_id = '{project_id}' AND
            site_id = '{site_id}' AND
            subject_id = '{subject_id}';
        """
        subject_df = db.execute_sql(config_file, query)

        if subject_df.empty:
            raise ValueError(
                f"No subject found with ID {subject_id} in project {project_id} and site {site_id}."
            )
        if len(subject_df) > 1:
            raise ValueError(
                "More than one subject found with ID "
                f"{subject_id} in project "
                f"{project_id} and site "
                f"{site_id}."
            )
        row = subject_df.iloc[0]
        return Subject(
            subject_id=row["subject_id"],
            site_id=row["site_id"],
            project_id=row["project_id"],
            subject_metadata=row["subject_metadata"],
        )

    @staticmethod
    def get_by_filter(
        project_id: str,
        site_id: str,
        config_file: Path,
        filters: Optional[Dict[str, Any]] = None,
    ) -> List["Subject"]:
        """Retrieves a list of subjects matching specified metadata filters.

        This function queries the database for subjects belonging to a given
        project and site, and then applies additional, dynamic filters to the
        `subject_metadata` JSONB column.

        Args:
            project_id (str): The project ID to filter by.
            site_id (str): The site ID to filter by.
            config_file (Path): Path to the database configuration file.
            filters (Optional[Dict[str, Any]]): A dictionary of metadata filters.
                - The `key` is a dot-separated string representing the path to a
                  nested JSON field (e.g., "cantab.CANTAB_Test_DS.cantab_id").
                - The `value` is the condition to meet:
                    - To match a specific value (str, int, bool), provide that value.
                    - To check for the *existence* of a key path, use `None` as the value.

        Returns:
            List[Subject]: A list of `Subject` objects that match the criteria.
                           Returns an empty list if no matches are found.

        Raises:
            ValueError: If a filter key is not a valid, non-empty string.

        .. note::
            This function constructs a raw SQL query using f-strings for filter
            values. While suitable for trusted inputs, a future refactoring should
            implement parameterized queries to prevent any risk of SQL injection.

        Usage Examples:
        ```python
        # 1. Find all subjects that have a 'CANTAB_Test_DS' record
        test_ds_filter = {"cantab.CANTAB_Test_DS": None}
        subjects = Subject.get_by_filter(..., filters=test_ds_filter)

        # 2. Find a subject by a specific, nested cantab_id
        specific_id_filter = {"cantab.CANTAB_Prod_DS.cantab_id": "689df5673991d9b8fb392b5p"}
        subjects = Subject.get_by_filter(..., filters=specific_id_filter)

        # 3. Find all subjects where the top-level 'testing' flag is true
        testing_filter = {"testing": 'true'}
        subjects = Subject.get_by_filter(..., filters=testing_filter)

        # 4. Combine filters (find subjects with Test DS data AND testing=True)
        combined_filter = {
            "cantab.CANTAB_Test_DS": None,
            "testing": 'true'
        }
        subjects = Subject.get_by_filter(..., filters=combined_filter)
        ```
        """
        # Start with the base query for project and site
        query = f"""
        SELECT *
        FROM subjects
        WHERE project_id = '{project_id}' AND site_id = '{site_id}'
        """

        # Dynamically append WHERE clauses for each metadata filter
        if filters:
            for path_str, value in filters.items():
                if not isinstance(path_str, str) or not path_str:
                    raise ValueError("Filter keys must be non-empty strings.")

                # Convert dot-notation path to a PostgreSQL path array for jsonb
                # e.g., "cantab.CANTAB_Test_DS" -> '{cantab,CANTAB_Test_DS}'
                path_list = path_str.split(".")
                sql_path = "{" + ",".join(path_list) + "}"

                if value is None:
                    # Use case: Check for the EXISTENCE of a key/path.
                    # The `#>` operator gets the JSON object at the specified path.
                    # We check if the result is not null, confirming the path exists.
                    query += f" AND subject_metadata #> '{sql_path}' IS NOT NULL"
                else:
                    # Use case: Check for a specific VALUE at a key/path.
                    # The `#>>` operator gets the field as TEXT.
                    # This allows for a simple string comparison.
                    query += f" AND subject_metadata #>> '{sql_path}' = '{str(value)}'"

        query += ";"

        subjects_df = db.execute_sql(config_file=config_file, query=query)

        if subjects_df.empty:
            return []

        # Reconstruct Subject objects from the resulting DataFrame
        subjects = []
        for _, row in subjects_df.iterrows():
            subjects.append(
                Subject(
                    subject_id=row["subject_id"],
                    site_id=row["site_id"],
                    project_id=row["project_id"],
                    subject_metadata=(
                        json.loads(row["subject_metadata"])
                        if isinstance(row["subject_metadata"], str)
                        else row["subject_metadata"]
                    ),
                )
            )
        return subjects

    @staticmethod
    def get_subjects_for_project_site(
        project_id: str, site_id: str, config_file: Path
    ) -> List["Subject"]:
        """
        Retrieves subjects for a given project and site from the database.

        Args:
            project_id (str): The project ID.
            site_id (str): The site ID.
            config_file (Path): Path to the configuration file.

        Returns:
            List[Subject]: A list of Subject objects.
        """
        query = f"""
        SELECT
            subject_id, site_id, project_id, subject_metadata
        FROM subjects
        WHERE project_id = '{project_id}' AND
            site_id = '{site_id}';
        """
        subjects_df = db.execute_sql(config_file, query)

        subjects: List[Subject] = []
        for _, row in subjects_df.iterrows():
            subject = Subject(
                subject_id=row["subject_id"],
                site_id=row["site_id"],
                project_id=row["project_id"],
                subject_metadata=row["subject_metadata"],
            )
            subjects.append(subject)
        return subjects

    def delete_record_query(self) -> str:
        """Generate a query to delete a record from the table"""
        query = f"""
        DELETE FROM subjects
        WHERE subject_id = '{self.subject_id}';
        """
        return query
    
    @staticmethod
    def get_subject_consent_date(
        project_id: str, site_id: str, subject_id: str, config_file: Path
    ) -> Optional[datetime]:
        """
        Retrieves a subject's consent date from the database.

        Args:
            project_id (str): The project ID.
            site_id (str): The site ID.
            subject_id (str): The subject ID.
            config_file (Path): Path to the configuration file.
        Returns:
            Optional[datetime]: The consent date if available, else None.
        """
        query = f"""
        SELECT subject_metadata->'consent_date' AS consent_date
        FROM subjects
        WHERE project_id = '{project_id}' AND
            site_id = '{site_id}' AND
            subject_id = '{subject_id}';
        """
        result = db.fetch_record(config_file, query)

        if result is None:
            return None

        consent_date_str: str = result
        if consent_date_str:
            return datetime.fromisoformat(consent_date_str)
        return None
