"""
Logs Model
"""

from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel

from lochness.helpers import db


class Logs(BaseModel):
    """
    Logs Model
    """

    log_level: Literal["DEBUG", "INFO", "WARN", "ERROR", "FATAL"]
    log_message: Dict[str, Any]
    log_timestamp: Optional[datetime] = None

    def __init__(self, **data):  # type: ignore
        super().__init__(**data)
        if self.log_timestamp is None:
            self.log_timestamp = datetime.now()

    @staticmethod
    def init_db_table_query() -> List[str]:
        """
        Returns the SQL query to create the database table for logs.
        """
        log_level_type = """
            CREATE TYPE log_level
            AS ENUM ('DEBUG', 'INFO', 'WARN', 'ERROR', 'FATAL');
        """
        sql_query = """
            CREATE TABLE logs (
                log_id SERIAL PRIMARY KEY,
                log_level log_level NOT NULL,
                log_message JSONB STORAGE EXTENDED NOT NULL,
                log_timestamp TIMESTAMPTZ DEFAULT NOW()
            );
        """
        index_queries = [
            "CREATE INDEX IF NOT EXISTS idx_logs_log_timestamp ON logs (log_timestamp);",
            "CREATE INDEX IF NOT EXISTS idx_logs_log_level ON logs (log_level);",
        ]

        init_query = [log_level_type, sql_query] + index_queries
        return init_query

    @staticmethod
    def drop_db_table_query() -> str:
        """
        Returns the SQL query to drop the database table for logs.
        """
        sql_query = """
            DROP TABLE IF EXISTS logs;
            DROP TYPE IF EXISTS log_level;
        """

        return sql_query

    def to_sql_query(self) -> str:
        """
        Converts the Logs instance to a SQL insert statement.
        """

        log_message = db.sanitize_json(self.log_message)

        sql_query = f"""
            INSERT INTO logs (
                log_level, log_message, log_timestamp
            ) VALUES (
                '{self.log_level}', '{log_message}', '{self.log_timestamp}'
            );
        """
        sql_query = db.handle_null(sql_query)
        return sql_query

    def insert(self, config_file: Path) -> None:
        """
        Inserts the log entry into the database.
        Args:
            config_file (Path): Path to the configuration file.
        """

        insert_query = self.to_sql_query()
        db.execute_queries(  # type: ignore
            config_file=config_file,
            queries=[insert_query],
            show_commands=False,
            silent=True,
        )
