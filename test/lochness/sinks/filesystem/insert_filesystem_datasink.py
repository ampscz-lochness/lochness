#!/usr/bin/env python
"""
Insert a DataSink for filesystem into the database.
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
from typing import Any, Dict

from rich.logging import RichHandler


from lochness.helpers import db, utils
from lochness.models.data_sinks import DataSink
from lochness.helpers import config

logger = logging.getLogger(__name__)
logargs: Dict[str, Any] = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}

logging.basicConfig(**logargs)


def main(config_file: Path):
    """
    Main function to insert a filesystem DataSink.
    """

    filesystem_creds = config.parse(
        config_file,
        'filesystem-datasink-test'
    )
    test_data_sink_name: str = filesystem_creds[
        'test_data_sink_name'
    ]  # type: ignore
    test_site_id: str = filesystem_creds[
        'test_site_id'
    ]  # type: ignore
    test_project_id: str = filesystem_creds[
        'test_project_id'
    ]  # type: ignore

    # This must match the key_name used when
    # inserting filesystem credentials
    keystore_name: str = filesystem_creds[
        'keystore_name'
    ]  # type: ignore

    logger.info(
        f"Ensuring filesystem data sink "
        f"'{test_data_sink_name}' exists..."
    )
    data_sink_metadata_for_insert = {
        "type": "filesystem",
        "keystore_name": keystore_name,
    }
    data_sink_obj = DataSink(
        data_sink_name=test_data_sink_name,
        site_id=test_site_id,
        project_id=test_project_id,
        data_sink_metadata=data_sink_metadata_for_insert,
        is_active=True,
    )

    db.execute_queries(
        config_file,
        [data_sink_obj.to_sql_query()],
        show_commands=False
    )
    logger.info(
        f"Filesystem data sink "
        f"'{test_data_sink_name}' inserted."
    )


if __name__ == "__main__":
    config_file = utils.get_config_file_path()
    if not config_file.exists():
        logger.error(f"ERROR: Configuration file not found at {config_file}")
        sys.exit(1)

    main(config_file=config_file)
