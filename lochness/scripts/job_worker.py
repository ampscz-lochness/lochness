#!/usr/bin/env python
"""
Job Worker Script for Lochness
Polls the jobs table for pending jobs and executes them (data_pull, data_push, etc.)
"""
import time
from datetime import datetime
from pathlib import Path
import logging
from lochness.helpers import config, db
from lochness.models.jobs import Job
from lochness.models.logs import Logs

# Import your pull_data and push_data logic here
from lochness.sources.redcap.tasks.pull_data import pull_all_data as redcap_pull_all_data
from lochness.sources.mindlamp.tasks.pull_data import pull_all_data as mindlamp_pull_all_data
from lochness.sources.cantab.tasks.pull_data import pull_all_data as cantab_pull_all_data
from lochness.sources.sharepoint.tasks.pull_data import pull_all_data as sharepoint_pull_all_data
from lochness.sources.terra.tasks.pull_data import pull_all_data as terra_pull_all_data
from lochness.sources.xnat.tasks.pull_data import pull_all_data as xnat_pull_all_data
from lochness.tasks.push_data import push_all_data

CONFIG_FILE = Path(__file__).resolve().parents[2] / "sample.config.ini"
POLL_INTERVAL = 10  # seconds

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("lochness.job_worker")

def get_next_pending_job():
    query = """
        SELECT * FROM jobs WHERE status = 'pending' ORDER BY created_at ASC LIMIT 1;
    """
    df = db.execute_sql(CONFIG_FILE, query)
    if not df.empty:
        return Job(**df.iloc[0].to_dict())
    return None

def update_job_status(job_id, status, started_at=None, finished_at=None, result=None):
    updates = [f"status = '{status}'"]
    if started_at:
        updates.append(f"started_at = '{started_at.isoformat()}'")
    if finished_at:
        updates.append(f"finished_at = '{finished_at.isoformat()}'")
    if result:
        safe_result = result.replace("'", "''")
        updates.append(f"result = '{safe_result}'")
    set_clause = ", ".join(updates)
    query = f"UPDATE jobs SET {set_clause} WHERE job_id = {job_id};"
    db.execute_queries(CONFIG_FILE, [query], show_commands=False)

def run_job(job: Job):
    logger.info(f"Running job {job.job_id}: {job.job_type} for project={job.project_id}, site={job.site_id}")
    update_job_status(job.job_id, 'running', started_at=datetime.now())
    try:
        if job.job_type == 'data_pull':
            # Determine which data source type to use
            data_source_type = None
            if job.job_metadata and isinstance(job.job_metadata, dict):
                data_source_type = job.job_metadata.get('data_source_type')
            if not data_source_type and job.data_source_name:
                # Fallback: try to infer from data_source_name
                name = job.data_source_name.lower()
                if 'redcap' in name:
                    data_source_type = 'redcap'
                elif 'mindlamp' in name:
                    data_source_type = 'mindlamp'
                elif 'cantab' in name:
                    data_source_type = 'cantab'
                elif 'sharepoint' in name:
                    data_source_type = 'sharepoint'
                elif 'terra' in name:
                    data_source_type = 'terra'
                elif 'xnat' in name:
                    data_source_type = 'xnat'
            if data_source_type == 'redcap':
                redcap_pull_all_data(CONFIG_FILE, project_id=job.project_id, site_id=job.site_id, push_to_sink=True)
            elif data_source_type == 'mindlamp':
                mindlamp_pull_all_data(CONFIG_FILE, project_id=job.project_id, site_id=job.site_id, push_to_sink=True)
            elif data_source_type == 'cantab':
                cantab_pull_all_data(CONFIG_FILE, project_id=job.project_id, site_id=job.site_id, push_to_sink=True)
            elif data_source_type == 'sharepoint':
                sharepoint_pull_all_data(CONFIG_FILE, project_id=job.project_id, site_id=job.site_id, push_to_sink=True)
            elif data_source_type == 'terra':
                terra_pull_all_data(CONFIG_FILE, project_id=job.project_id, site_id=job.site_id, push_to_sink=True)
            elif data_source_type == 'xnat':
                xnat_pull_all_data(CONFIG_FILE, project_id=job.project_id, site_id=job.site_id, push_to_sink=True)
            else:
                result = f"Unknown or missing data_source_type for data_pull: {data_source_type}"
                update_job_status(job.job_id, 'error', finished_at=datetime.now(), result=result)
                return
            result = f"Pulled data for {job.project_id}/{job.site_id}/{job.data_source_name} (type: {data_source_type})"
        elif job.job_type == 'data_push':
            push_all_data(CONFIG_FILE, project_id=job.project_id, site_id=job.site_id)
            result = f"Pushed data for {job.project_id}/{job.site_id}/{job.data_sink_name}"
        elif job.job_type == 'refresh_metadata':
            data_source_type = None
            if job.job_metadata and isinstance(job.job_metadata, dict):
                data_source_type = job.job_metadata.get('data_source_type')
            if not data_source_type and job.data_source_name:
                name = job.data_source_name.lower()
                if 'redcap' in name:
                    data_source_type = 'redcap'
                elif 'mindlamp' in name:
                    data_source_type = 'mindlamp'
                elif 'cantab' in name:
                    data_source_type = 'cantab'
                elif 'sharepoint' in name:
                    data_source_type = 'sharepoint'
                elif 'terra' in name:
                    data_source_type = 'terra'
                elif 'xnat' in name:
                    data_source_type = 'xnat'
            if data_source_type == 'redcap':
                from lochness.sources.redcap.tasks.refresh_metadata import refresh_all_metadata
                refresh_all_metadata(CONFIG_FILE, project_id=job.project_id, site_id=job.site_id)
                result = f"Refreshed metadata for {job.project_id}/{job.site_id}/{job.data_source_name} (type: redcap)"
            else:
                result = f"Unknown or unsupported data_source_type for refresh_metadata: {data_source_type}"
                update_job_status(job.job_id, 'error', finished_at=datetime.now(), result=result)
                return
        else:
            result = f"Unknown job type: {job.job_type}"
            update_job_status(job.job_id, 'error', finished_at=datetime.now(), result=result)
            return
        update_job_status(job.job_id, 'success', finished_at=datetime.now(), result=result)
        logger.info(f"Job {job.job_id} completed successfully.")
    except Exception as e:
        logger.error(f"Job {job.job_id} failed: {e}")
        update_job_status(job.job_id, 'error', finished_at=datetime.now(), result=str(e))
        Logs(
            log_level="ERROR",
            log_message={
                "event": "job_failed",
                "job_id": job.job_id,
                "error": str(e),
            },
        ).insert(CONFIG_FILE)

def main():
    logger.info("Starting Lochness Job Worker...")
    while True:
        job = get_next_pending_job()
        if job:
            run_job(job)
        else:
            time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    main() 