"""
Metrics for monitoring the load and performance of the Lochness systems.
"""

from typing import Any, Dict

from pydantic import BaseModel

from lochness.helpers import db


class Metrics(BaseModel):
    """
    Metrics for monitoring the load and performance of the Lochness systems.
    """

    metric_source: str
    metric_name: str
    metric_payload: Dict[str, Any]

    @staticmethod
    def init_db_table_query() -> str:
        """
        Returns the SQL query to create the database table for metrics.
        """
        sql_query = """
            CREATE TABLE metrics (
                metric_source TEXT NOT NULL,
                metric_name TEXT NOT NULL,
                metric_payload JSONB NOT NULL,
                metric_timestamp TIMESTAMPTZ DEFAULT NOW(),
                PRIMARY KEY (metric_source, metric_name, metric_timestamp)
            );
        """

        return sql_query

    @staticmethod
    def drop_db_table_query() -> str:
        """
        Returns the SQL query to drop the database table for metrics.
        """
        sql_query = """
            DROP TABLE IF EXISTS metrics;
        """

        return sql_query

    def to_sql_query(self) -> str:
        """
        Returns the SQL query to insert the metrics into the database.
        """
        metric_source = db.sanitize_string(self.metric_source)
        metric_name = db.sanitize_string(self.metric_name)
        metric_payload = db.sanitize_json(self.metric_payload)

        sql_query = f"""
            INSERT INTO metrics (metric_source, metric_name, metric_payload)
            VALUES ('{metric_source}', '{metric_name}', '{metric_payload}');
        """

        return sql_query
