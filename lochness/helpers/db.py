"""
Helper functions for interacting with a PostgreSQL database.
"""

import json
import logging
import sys
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple, no_type_check

import pandas as pd
import psycopg2
import sqlalchemy

from lochness.helpers import config, utils

logger = logging.getLogger(__name__)


def handle_null(query: str) -> str:
    """
    Replaces all occurrences of the string 'NULL' with the SQL NULL keyword in the given query.

    Args:
        query (str): The SQL query to modify.

    Returns:
        str: The modified SQL query with 'NULL' replaced with NULL.
    """
    query = query.replace("'NULL'", "NULL")

    return query


def handle_nan(query: str) -> str:
    """
    Replaces all occurrences of the string 'nan' with the SQL NULL keyword in the given query.

    Args:
        query (str): The SQL query to modify.

    Returns:
        str: The modified SQL query with 'nan' replaced with NULL.
    """
    query = query.replace("'nan'", "NULL")

    return query


def sanitize_string(string: str) -> str:
    """
    Sanitizes a string by escaping single quotes.

    Args:
        string (str): The string to sanitize.

    Returns:
        str: The sanitized string.
    """
    return string.replace("'", "''")


def sanitize_json(json_dict: Dict[str, Any]) -> str:
    """
    Sanitizes a JSON object by replacing single quotes with double quotes.

    Args:
        json_dict (dict): The JSON object to sanitize.

    Returns:
        str: The sanitized JSON object.
    """
    for key, value in json_dict.items():
        if isinstance(value, str):
            json_dict[key] = sanitize_string(value)

    json_str = json.dumps(json_dict, default=str)

    # Replace NaN with NULL
    json_str = json_str.replace("NaN", "null")

    return json_str


def get_db_credentials(config_file: Path, db: str = "postgresql") -> Dict[str, str]:
    """
    Retrieves the database credentials from the configuration file.

    Args:
        config_file (Path): The path to the configuration file.
        db (str, optional): The section of the configuration file to use.
            Defaults to "postgresql".

    Returns:
        Dict[str, str]: A dictionary containing the database credentials.
    """
    db_params = config.parse(path=config_file, section=db)

    if "key_file" in db_params:
        key_file_str: str = db_params["key_file"]  # type: ignore
        key_file = Path(key_file_str)
        credentials = config.parse(path=key_file, section=db)
    else:
        credentials = db_params

    return credentials  # type: ignore


@no_type_check
def execute_queries(
    config_file: Path,
    queries: List[str],
    show_commands: bool = True,
    show_progress: bool = False,
    silent: bool = False,
    db: str = "postgresql",
    on_failure: Optional[Callable[[], None]] = sys.exit,
) -> List[Tuple[Any, ...]]:
    """
    Executes a list of SQL queries on a PostgreSQL database.

    Args:
        config_file_path (str): The path to the configuration file containing
            the connection parameters.
        queries (list): A list of SQL queries to execute.
        show_commands (bool, optional): Whether to display the executed SQL queries.
            Defaults to True.
        show_progress (bool, optional): Whether to display a progress bar. Defaults to False.
        silent (bool, optional): Whether to suppress output. Defaults to False.
        db (str, optional): The section of the configuration file to use.
            Defaults to "postgresql".
        backup (bool, optional): Whether to sace all executed queries to a file.

    Returns:
        list: A list of tuples containing the results of the executed queries.
    """
    command = None
    output: List[Tuple[Any, ...]] = []

    try:
        credentials = get_db_credentials(config_file=config_file, db=db)
        conn: psycopg2.extensions.connection = psycopg2.connect(**credentials)
        cur = conn.cursor()

        def execute_query(query: str):
            if show_commands:
                logger.debug("Executing query:")
                logger.debug(f"[bold blue]{query}", extra={"markup": True})
            cur.execute(query)  # type: ignore
            try:
                output.append(cur.fetchall())  # type: ignore
            except psycopg2.ProgrammingError:
                pass

        if show_progress:
            with utils.get_progress_bar() as progress:
                task = progress.add_task("Executing SQL queries...", total=len(queries))

                for command in queries:
                    progress.update(task, advance=1)
                    execute_query(command)

        else:
            for command in queries:
                execute_query(command)

        cur.close()

        conn.commit()

        if not silent:
            logger.debug(
                f"[grey]Executed {len(queries)} SQL query(ies).", extra={"markup": True}
            )
    except (Exception, psycopg2.DatabaseError) as e:  # pylint: disable=broad-except
        logger.error("[bold red]Error executing queries.", extra={"markup": True})
        if command is not None:
            logger.error(f"[red]For query: {command}", extra={"markup": True})
        logger.error(e)

        # Check for specific connection error related to hostname resolution
        if isinstance(
            e, psycopg2.OperationalError
        ) and "could not translate host name" in str(e):
            logger.error(
                "HINT: This error often indicates a network issue.",
                extra={"markup": True},
            )

        if on_failure is not None:
            on_failure()
        else:
            raise e
    finally:
        if conn is not None:
            conn.close()

    return output


def get_db_connection(
    config_file: Path, db: str = "postgresql"
) -> sqlalchemy.engine.base.Engine:
    """
    Establishes a connection to the PostgreSQL database using the provided configuration file.

    Args:
        config_file (Path): The path to the configuration file.

    Returns:
        sqlalchemy.engine.base.Engine: The database connection engine.
    """
    credentials = get_db_credentials(config_file=config_file, db=db)
    engine = sqlalchemy.create_engine(
        "postgresql+psycopg2://"
        + credentials["user"]
        + ":"
        + credentials["password"]
        + "@"
        + credentials["host"]
        + ":"
        + credentials["port"]
        + "/"
        + credentials["database"]
    )

    return engine  # type: ignore


def execute_sql(
    config_file: Path, query: str, db: str = "postgresql", debug: bool = False
) -> pd.DataFrame:
    """
    Executes a SQL query on a PostgreSQL database and returns the result as a pandas DataFrame.

    Args:
        config_file_path (str): The path to the configuration file containing the
            PostgreSQL database credentials.
        query (str): The SQL query to execute.

    Returns:
        pd.DataFrame: A pandas DataFrame containing the result of the SQL query.
    """
    engine = get_db_connection(config_file=config_file, db=db)

    if debug:
        logger.debug(f"Executing query: {query}")

    df: pd.DataFrame = pd.read_sql(query, engine)  # type: ignore

    engine.dispose()

    return df


def fetch_record(
    config_file: Path, query: str, db: str = "postgresql"
) -> Optional[Any]:
    """
    Fetches a single record from the database using the provided SQL query.

    Args:
        config_file_path (str): The path to the database configuration file.
        query (str): The SQL query to execute.

    Returns:
        Optional[str]: The value of the first column of the first row of the result set,
        or None if the result set is empty.
    """
    df = execute_sql(config_file=config_file, query=query, db=db)

    # Check if there is a row
    if df.shape[0] == 0:
        return None

    value = df.iloc[0, 0]
    if isinstance(value, tuple) and len(value) == 1:
        return value[0]
    return value
