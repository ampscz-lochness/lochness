"""
Logging configuration
"""

import logging
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from rich.logging import RichHandler

from lochness.helpers import config, utils
from lochness.logs.handlers import BatchedPostgresLogHandler

logger = logging.getLogger(__name__)


def configure_logging(
    config_file: Path,
    module_name: str,
    logger: logging.Logger,
    level: int = logging.DEBUG,
    use_db: bool = True,
    noisy_modules: Optional[List[str]] = None,
    noisy_level: int = logging.INFO,
) -> None:
    """
    Configures logging for a given module using the specified configuration file.

    Sets up both file logging (from config) and console logging with appropriate
    handlers based on execution context:
    - CLI context: Uses RichHandler for colorful, interactive output
    - Airflow context: Uses standard StreamHandler for plain text logs

    Args:
        config_file (Path): The path to the configuration file.
        module_name (str): The name of the module to configure logging for.
        logger (logging.Logger): The logger object to use for logging.
        level (int): The logging level. Defaults to logging.DEBUG.
        use_db (bool): Whether to use the database logging handler. Defaults to True.

    Returns:
        None
    """
    # Set up console handler based on execution context
    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    # Remove existing handlers to avoid duplicates
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    if utils.is_running_in_airflow():
        # Airflow context: use standard logging format for easier parsing
        console_handler = logging.StreamHandler()
        console_handler.setLevel(level)
        console_handler.setFormatter(
            logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        )
        root_logger.addHandler(console_handler)
    else:
        # CLI context: use Rich for colorful output
        console_handler = RichHandler(rich_tracebacks=True)
        console_handler.setLevel(level)
        root_logger.addHandler(console_handler)

    # Set up file logging from config
    log_params = config.parse(config_file, "logging")
    log_file_r: str = log_params[module_name]  # type: ignore

    if log_file_r.startswith("/"):
        log_file = Path(log_file_r)
    else:
        general_params = config.parse(config_file, "general")
        repo_root = Path(general_params["repo_root"])  # type: ignore

        log_file = repo_root / log_file_r

    if log_file.exists() and log_file.stat().st_size > 10000000:  # 10MB
        archive_file = (
            log_file.parent
            / "archive"
            / f"{log_file.stem}_{datetime.now().strftime('%Y%m%d%H%M%S')}.log"
        )
        logger.info(f"Rotating log file to {archive_file}")

        archive_file.parent.mkdir(parents=True, exist_ok=True)
        log_file.rename(archive_file)

    # Ensure log directory exists
    log_file.parent.mkdir(parents=True, exist_ok=True)

    file_handler = logging.FileHandler(log_file, mode="a")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(
        logging.Formatter(
            "%(asctime)s  - "
            "%(process)d - "
            "%(name)s - "
            "%(levelname)s - "
            "%(message)s - "
            "[%(filename)s:%(lineno)d]"
        )
    )

    root_logger.addHandler(file_handler)
    logger.info(f"Logging to {log_file}")

    if use_db:
        db_handler = BatchedPostgresLogHandler(config_file=config_file)
        db_handler.setLevel(logging.DEBUG)
        root_logger.addHandler(db_handler)
        logger.info("Logging to PostgreSQL database")

    if noisy_modules is not None:
        silence_logs(noisy_modules, noisy_level)


def silence_logs(noisy_modules: List[str], target_level: int = logging.INFO) -> None:
    """
    Silences logs from specified modules.

    Args:
        noisy_modules (List[str]): A list of modules to silence.
        target_level (int): The target log level to set the modules to.

    Returns:
        None
    """
    for module in noisy_modules:
        logger.debug(f"Setting log level for {module} to {target_level}")
        logging.getLogger(module).setLevel(target_level)
