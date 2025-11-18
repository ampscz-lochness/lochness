"""
Data Source Model for SharePoint
"""

from pathlib import Path
from typing import Any, Dict, List

from pydantic import BaseModel

from lochness.helpers import db


class SharepointDataSourceMetadata(BaseModel):
    """
    Metadata for a SharePoint data source.
    """

    keystore_name: str
    site_url: str
    form_name: str
    form_title: str
    modality: str
    drive_name: str


class SharepointDataSource(BaseModel):
    """
    A SharePoint data source is a specific type of data source that Lochness can connect to
    and pull data from.
    """

    data_source_name: str
    is_active: bool
    site_id: str
    project_id: str
    data_source_type: str
    data_source_metadata: SharepointDataSourceMetadata

    @staticmethod
    def get_all_sharepoint_data_sources(
        config_file: Path,
        active_only: bool = True,
    ) -> List["SharepointDataSource"]:
        """
        Get all active SharePoint data sources.

        Returns:
            List[SharepointDataSource]: A list of active SharePoint data sources.
        """
        sql_query = """
            SELECT *
            FROM data_sources
            WHERE data_source_type = 'sharepoint'
        """

        if active_only:
            sql_query += " AND data_source_is_active = TRUE"

        df = db.execute_sql(
            config_file=config_file,
            query=sql_query,
        )

        def convert_to_sharepoint_data_source(row: Dict[str, Any]) -> "SharepointDataSource":
            """
            Convert a row from the data_sources table to a SharepointDataSource object.

            Args:
                row (Dict[str, Any]): A dictionary representing a row from the data_sources table.

            Returns:
                SharepointDataSource: A SharepointDataSource object.
            """
            sharepoint_data_source = SharepointDataSource(
                data_source_name=row["data_source_name"],
                is_active=row["data_source_is_active"],
                site_id=row["site_id"],
                project_id=row["project_id"],
                data_source_type=row["data_source_type"],
                data_source_metadata=SharepointDataSourceMetadata(
                    keystore_name=row["data_source_metadata"]["keystore_name"],
                    site_url=row["data_source_metadata"]["site_url"],
                    form_title=row["data_source_metadata"]["form_title"],
                    form_name=row["data_source_metadata"]["form_name"],
                    modality=row["data_source_metadata"]["modality"],
                    drive_name=row["data_source_metadata"]["drive_name"],
                ),
            )
            return sharepoint_data_source

        sharepoint_data_sources: List[SharepointDataSource] = []

        for _, row in df.iterrows():  # type: ignore
            sharepoint_data_source = convert_to_sharepoint_data_source(row.to_dict())  # type: ignore
            sharepoint_data_sources.append(sharepoint_data_source)

        return sharepoint_data_sources
