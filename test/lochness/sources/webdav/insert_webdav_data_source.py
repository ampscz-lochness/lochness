"""
Insert a test WebDAV data source into the database.
"""

import sys
from pathlib import Path

file = Path(__file__).resolve()
parent = file.parent
root_dir = None  # pylint: disable=invalid-name
for parent in file.parents:
    if parent.name == "lochness_v2":
        root_dir = parent

sys.path.append(str(root_dir))

# remove current directory from path
try:
    sys.path.remove(str(parent))
except ValueError:
    pass

import logging
from typing import Dict, Any
import json

from rich.logging import RichHandler

from lochness.helpers import utils, db, config
from lochness.models.data_source import DataSource
from lochness.sources.webdav.models.data_source import (
    WebDavDataSourceMetadata,
)

MODULE_NAME = "test.lochness.sources.webdav.insert_data_source"

logger = logging.getLogger(MODULE_NAME)
logargs: Dict[str, Any] = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}

logging.basicConfig(**logargs)


def insert_webdav_data_source():
    """
    Insert a test CANTAB data source into the database.
    """
    config_file = utils.get_config_file_path()
    data_source_config = config.parse(config_file, "webdav-test")

    # Create the data source metadata
    data_source_metadata = WebDavDataSourceMetadata(
        keystore_name=data_source_config["key_name"],  # type: ignore
        endpoint_url=data_source_config["endpoint_url"],  # type: ignore
        match_prefix=data_source_config["match_prefix"],  # type: ignore
        match_postfix=data_source_config["match_postfix"],  # type: ignore
        file_datastructure=data_source_config["file_datastructure"],  # type: ignore
        file_datastructure_metadata=json.loads(
            data_source_config["file_datastructure_metadata"]  # type: ignore
        ),
        modality=data_source_config["modality"],  # type: ignore
    )

    # Create the data source
    data_source: DataSource = DataSource(
        data_source_name=data_source_config["data_source_name"],  # type: ignore
        site_id=data_source_config["site_id"],  # type: ignore
        project_id=data_source_config["project_id"],  # type: ignore
        data_source_type="webdav",
        is_active=True,
        data_source_metadata=data_source_metadata.model_dump(),
    )

    db.execute_queries(config_file, [data_source.to_sql_query()], show_commands=True)

    logger.info(
        f"Successfully inserted WebDAV data source: {data_source_config['data_source_name']}"
    )
    logger.info(f"Site: {data_source_config['site_id']}")
    logger.info(f"Project: {data_source_config['project_id']}")
    logger.info(f"Modality: {data_source_config['modality']}")


if __name__ == "__main__":
    insert_webdav_data_source()
