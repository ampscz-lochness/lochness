from pathlib import Path
from typing import List, Dict, Any

from pydantic import BaseModel

from lochness.helpers import db


class WebDavDataSourceMetadata(BaseModel):
    """
    Metadata for a WebDAV data source.
    """

    keystore_name: str
    endpoint_url: str
    match_prefix: str
    match_postfix: str
    file_datastructure: str
    file_datastructure_metadata: Dict[str, Any]
    modality: str


class WebDavDataSource(BaseModel):
    """
    A WebDAV data source is a specific type of data source that Lochness can connect to
    and pull data from.
    """

    data_source_name: str
    is_active: bool
    site_id: str
    project_id: str
    data_source_type: str
    data_source_metadata: WebDavDataSourceMetadata

    @staticmethod
    def get_all_webdav_data_sources(
        config_file: Path,
        active_only: bool = True,
    ) -> List["WebDavDataSource"]:
        """
        Get all active WebDAV data sources.

        Returns:
            List[WebDavDataSource]: A list of active WebDAV data sources.
        """
        sql_query = """
            SELECT *
            FROM data_sources
            WHERE data_source_type = 'webdav'
        """

        if active_only:
            sql_query += " AND data_source_is_active = TRUE"

        df = db.execute_sql(
            config_file=config_file,
            query=sql_query,
        )

        data_sources = []
        for _, row in df.iterrows():
            metadata_dict = row["data_source_metadata"]
            metadata = WebDavDataSourceMetadata(**metadata_dict)

            data_source = WebDavDataSource(
                data_source_name=row["data_source_name"],
                is_active=row["data_source_is_active"],
                site_id=row["site_id"],
                project_id=row["project_id"],
                data_source_type=row["data_source_type"],
                data_source_metadata=metadata,
            )
            data_sources.append(data_source)

        return data_sources
