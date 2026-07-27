"""
Job Model for unified job queue (pull, push, etc.)
"""
import json
from datetime import datetime
from typing import Any, Dict, Optional

from pydantic import BaseModel


class Job(BaseModel):
    job_id: Optional[int] = None
    job_type: str  # e.g., 'data_pull', 'data_push', 'custom'
    project_id: str
    site_id: str
    data_source_name: Optional[str] = None
    data_sink_name: Optional[str] = None
    requested_by: Optional[str] = None
    status: str = 'pending'  # pending, running, success, error
    created_at: Optional[datetime] = None
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    result: Optional[str] = None
    job_metadata: Optional[Dict[str, Any]] = None

    @staticmethod
    def init_db_table_query() -> str:
        return """
        CREATE TABLE IF NOT EXISTS jobs (
            job_id SERIAL PRIMARY KEY,
            job_type TEXT NOT NULL,
            project_id TEXT NOT NULL,
            site_id TEXT NOT NULL,
            data_source_name TEXT,
            data_sink_name TEXT,
            requested_by TEXT,
            status TEXT NOT NULL DEFAULT 'pending',
            created_at TIMESTAMPTZ DEFAULT NOW(),
            started_at TIMESTAMPTZ,
            finished_at TIMESTAMPTZ,
            result TEXT,
            job_metadata JSONB
        );
        """

    def to_sql_insert_query(self) -> str:
        # This is a simplified insert; in production use parameterized queries
        if not self.job_metadata:
            job_metadata = "NULL"
        else:
            # Safely convert job_metadata dict to a JSON string and escape single quotes
            job_metadata_json = json.dumps(self.job_metadata).replace("'", "''")
            job_metadata = f"'{job_metadata_json}'"
        return f"""
        INSERT INTO jobs (
            job_type, project_id, site_id, data_source_name, data_sink_name, requested_by, status, job_metadata
        ) VALUES (
            '{self.job_type}', '{self.project_id}', '{self.site_id}',
            {f"'{self.data_source_name}'" if self.data_source_name else 'NULL'},
            {f"'{self.data_sink_name}'" if self.data_sink_name else 'NULL'},
            {f"'{self.requested_by}'" if self.requested_by else 'NULL'},
            '{self.status}',
            {job_metadata}
        );
        """ 