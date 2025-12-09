#!/usr/bin/env python
"""
Imports configuration from config.json into the database.
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

import argparse
import json
import logging
from typing import Any, Dict, List

from rich.logging import RichHandler

from lochness.helpers import logs, utils, db, config
from lochness.models.projects import Project
from lochness.models.sites import Site
from lochness.models.subjects import Subject
from lochness.models.data_source import DataSource
from lochness.models.data_sinks import DataSink
from lochness.models.keystore import KeyStore

MODULE_NAME = "lochness.scripts.import_setup_json"

console = utils.get_console()

logger = logging.getLogger(MODULE_NAME)
logargs: Dict[str, Any] = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}

logging.basicConfig(**logargs)


def import_setup_json(setup_json: Path, config_file: Path) -> None:
    """
    Import configuration from setup_json into the database.

    Args:
        setup_json (Path): Path to the setup JSON file.
        config_file (Path): Path to the configuration file.
    """

    logger.info(f"Importing setup from JSON file: {setup_json}")
    if not setup_json.exists():
        logger.error(f"Setup JSON file does not exist: {setup_json}")
        sys.exit(1)

    setup_data: Dict[str, Any] = json.loads(setup_json.read_text())

    queries: List[str] = []

    if "project" in setup_data:
        project = Project(**setup_data["project"])
        queries.append(project.to_sql_query())

    for site_data in setup_data.get("sites", []):
        site = Site(**site_data)
        queries.append(site.to_sql_query())

    for subject_data in setup_data.get("subjects", []):
        subject = Subject(**subject_data)
        queries.append(subject.to_sql_query())

    for ds_data in setup_data.get("data_sources", []):
        data_source = DataSource(**ds_data)
        queries.append(data_source.to_sql_query())

    for sink_data in setup_data.get("data_sinks", []):
        data_sink = DataSink(**sink_data)
        queries.append(data_sink.to_sql_query())

    encryption_passphrase = config.get_encryption_passphrase(config_file)
    for cred_data in setup_data.get("credentials", []):
        if "key_value" in cred_data and not isinstance(cred_data["key_value"], str):
            cred_data["key_value"] = json.dumps(cred_data["key_value"])
        keystore = KeyStore(**cred_data)
        queries.append(keystore.to_sql_query(encryption_passphrase))

    # Execute all queries
    db.execute_queries(config_file, queries, show_commands=True)

    logger.info("Import completed successfully.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Import configuration from config.json into the database."
    )
    parser.add_argument(
        "--setup-json",
        "-s",
        type=Path,
        help="Path to the setup JSON file",
    )
    parser.add_argument(
        "--config",
        "-c",
        type=Path,
        default=utils.get_config_file_path(),
        help="Path to the configuration file (default: config.json in current directory)",
    )
    args = parser.parse_args()

    config_file: Path = Path(args.config)
    logs.configure_logging(
        config_file=config_file, module_name=MODULE_NAME, logger=logger, use_db=False
    )

    setup_json: Path = args.setup_json
    if setup_json is None:
        logger.error("Please provide the path to the setup JSON file using --setup-json or -s")
        sys.exit(1)
    setup_json = Path(setup_json).resolve()

    console.rule(f"[bold red]{MODULE_NAME}")
    logger.info(f"Using config file: {config_file}")
    if not config_file.exists():
        logger.error(f"Config file does not exist: {config_file}")
        sys.exit(1)

    import_setup_json(setup_json=setup_json, config_file=config_file)

    logger.info("Done!")
