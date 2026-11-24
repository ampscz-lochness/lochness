"""
Data Source Model for Terra (app.terra.bio on Google Cloud Platform).
"""

from pathlib import Path
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field, field_validator

from lochness.helpers import db

DEFAULT_TERRA_API_URL = "https://api.firecloud.org"


class TerraDataSourceMetadata(BaseModel):
    """
    Metadata for a Terra data source.

    Supports pulling workspace entity data tables, GCS files referenced by
    ``gs://`` URIs in those tables, subject-scoped GCS bucket folders, and/or
    entire workspace GCS buckets.
    """

    workspace_namespace: str
    workspace_name: str
    keystore_name: Optional[str] = None
    modality: str = "terra"
    api_url: str = DEFAULT_TERRA_API_URL

    # Pull mode flags
    pull_tables: bool = True
    pull_table_gcs_files: bool = True
    pull_bucket_files: bool = False
    pull_workspace_bucket: bool = False

    # Entity table options
    entity_types: Optional[List[str]] = None
    table_format: Literal["json", "tsv", "csv"] = "json"
    subject_id_column: str = "name"
    subject_id_regex: Optional[str] = None
    fallback_subject_id: Optional[str] = None
    gcs_uri_columns: Optional[List[str]] = None

    # Optional Lochness subject_metadata key for mapping subject_id -> Terra ID
    subject_id_metadata_key: Optional[str] = None

    # Direct GCS bucket sync options
    bucket_name: Optional[str] = None
    bucket_prefix: str = ""
    bucket_subject_folder_template: str = "{subject_id}"
    include_patterns: Optional[List[str]] = None
    exclude_patterns: Optional[List[str]] = None

    # Optional GCP project for Requester Pays GCS buckets
    billing_project: Optional[str] = None

    # Parallelism for GCS metadata/download worker pool
    max_workers: int = Field(default=16, ge=1)

    @field_validator("api_url")
    @classmethod
    def strip_trailing_slash(cls, value: str) -> str:
        """Normalize API URL by stripping any trailing slash."""
        return value.rstrip("/")


class TerraDataSource(BaseModel):
    """
    A Terra data source is a specific type of data source that Lochness can
    connect to and pull data tables and GCS files from.
    """

    data_source_name: str
    is_active: bool
    site_id: str
    project_id: str
    data_source_type: str
    data_source_metadata: TerraDataSourceMetadata

    @staticmethod
    def get_all_terra_data_sources(
        config_file: Path,
        active_only: bool = True,
    ) -> List["TerraDataSource"]:
        """
        Get all Terra data sources from the database.

        Args:
            config_file (Path): Path to the Lochness configuration file.
            active_only (bool): If True, return only active data sources.

        Returns:
            List[TerraDataSource]: Matching Terra data sources.
        """
        sql_query = """
            SELECT *
            FROM data_sources
            WHERE data_source_type = 'terra'
        """

        if active_only:
            sql_query += " AND data_source_is_active = TRUE"

        df = db.execute_sql(
            config_file=config_file,
            query=sql_query,
        )

        def convert_to_terra_data_source(row: Dict[str, Any]) -> "TerraDataSource":
            raw_metadata = row["data_source_metadata"] or {}
            return TerraDataSource(
                data_source_name=row["data_source_name"],
                is_active=row["data_source_is_active"],
                site_id=row["site_id"],
                project_id=row["project_id"],
                data_source_type=row["data_source_type"],
                data_source_metadata=TerraDataSourceMetadata(**raw_metadata),
            )

        terra_data_sources: List[TerraDataSource] = []

        for _, row in df.iterrows():  # type: ignore
            terra_data_source = convert_to_terra_data_source(row.to_dict())  # type: ignore
            terra_data_sources.append(terra_data_source)

        return terra_data_sources

    @staticmethod
    def get(
        data_source_name: str,
        site_id: str,
        project_id: str,
        config_file: Path,
    ) -> Optional["TerraDataSource"]:
        """
        Get a specific Terra data source by name, site ID, and project ID.

        Args:
            data_source_name (str): The name of the data source.
            site_id (str): The site ID.
            project_id (str): The project ID.
            config_file (Path): Path to the Lochness configuration file.

        Returns:
            Optional[TerraDataSource]: The Terra data source if found, else None.
        """
        all_data_sources = TerraDataSource.get_all_terra_data_sources(
            config_file=config_file,
            active_only=False,
        )

        for data_source in all_data_sources:
            if (
                data_source.data_source_name == data_source_name
                and data_source.site_id == site_id
                and data_source.project_id == project_id
            ):
                return data_source

        return None
