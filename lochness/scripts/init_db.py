#!/usr/bin/env python
"""
Initializes the database.
"""
import sys
from pathlib import Path

file = Path(__file__).resolve()
parent = file.parent
root_dir = None  # pylint: disable=invalid-name
for ancestor in file.parents:
    if ancestor.name in ("lochness_v2", "lochness") and (ancestor / "lochness").is_dir():
        root_dir = ancestor
        break
if root_dir is None:
    root_dir = file.parents[2]

if str(root_dir) not in sys.path:
    sys.path.append(str(root_dir))

# remove current directory from path
try:
    sys.path.remove(str(parent))
except ValueError:
    pass

import logging
from typing import Any, Dict

from rich.logging import RichHandler

from lochness.helpers import logs, utils
from lochness.models import init_db

MODULE_NAME = "lochness.scripts.init_db"

console = utils.get_console()

logger = logging.getLogger(MODULE_NAME)
logargs: Dict[str, Any] = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}

logging.basicConfig(**logargs)


if __name__ == "__main__":
    config_file = utils.get_config_file_path()
    logs.configure_logging(
        config_file=config_file, module_name=MODULE_NAME, logger=logger, use_db=False
    )

    console.rule(f"[bold red]{MODULE_NAME}")
    logger.info(f"Using config file: {config_file}")
    if not config_file.exists():
        logger.error(f"Config file does not exist: {config_file}")
        sys.exit(1)

    init_db(config_file=config_file)
    logger.info("Done!")
