"""
Helper functions for the pipeline
"""

import os
from pathlib import Path
from datetime import datetime

from rich.console import Console
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)
import pandas as pd

from lochness.helpers import cli

_console = Console(color_system="standard")


def is_running_in_airflow() -> bool:
    """
    Detect if the script is running in an Airflow context.

    Checks for common Airflow environment variables that are set when
    a task is executed by Airflow.

    Returns:
        bool: True if running in Airflow, False otherwise.
    """
    airflow_env_vars = [
        "AIRFLOW_CTX_DAG_ID",
        "AIRFLOW_CTX_TASK_ID",
        "AIRFLOW_CTX_EXECUTION_DATE",
        "AIRFLOW_CTX_DAG_RUN_ID",
        "AIRFLOW_HOME",
    ]
    return any(var in os.environ for var in airflow_env_vars)


def get_progress_bar(transient: bool = False) -> Progress:
    """
    Returns a rich Progress object with standard columns.

    Returns:
        Progress: A rich Progress object with standard columns.
    """
    return Progress(
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        MofNCompleteColumn(),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
        transient=transient,
    )


def get_console() -> Console:
    """
    Returns a Console object with standard color system.

    Returns:
        Console: A Console object with standard color system.
    """
    return _console


def get_config_file_path() -> Path:
    """
    Returns the path to the config file.

    Returns:
        str: The path to the config file.

    Raises:
        ConfigFileNotFoundExeption: If the config file is not found.
    """
    repo_root = cli.get_repo_root()
    config_file_path = repo_root + "/config.ini"

    # Check if config_file_path exists
    if not Path(config_file_path).is_file():
        raise FileNotFoundError(f"Config file not found at {config_file_path}")

    return Path(config_file_path)


def get_timestamp() -> str:
    """
    Returns the current timestamp as a string in YYYYMMDD_HHMMSS format.
    """
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def explode_col(df: pd.DataFrame, col: str) -> pd.DataFrame:
    """
    Explodes the `col` column of the DataFrame.

    `col` column contains a JSON object.

    Args:
        df (pd.DataFrame): DataFrame containing the `col`.
        col (str, optional): The name of the column to explode. Defaults to "form_data".

    Returns:
        pd.DataFrame: DataFrame with the `col` column exploded.
    """
    df.reset_index(drop=True, inplace=True)
    df = pd.concat(
        [df.drop(col, axis=1), pd.json_normalize(df[col])],  # type: ignore
        axis=1,
    )

    return df
