#!/usr/bin/env python
"""
Pulls data from XNAT for active data sources and subjects.

This script is intended to be run as a cron job.
It will pull data for all active XNAT data sources and their associated subjects.
"""

import sys
from pathlib import Path
import argparse
import logging
from typing import Any, Dict, List, Optional, cast
from datetime import datetime

import xnat
import requests
from rich.logging import RichHandler

from lochness.helpers import logs, utils, db, config
from lochness.models.subjects import Subject
from lochness.models.keystore import KeyStore
from lochness.models.logs import Logs
from lochness.models.files import File
from lochness.models.data_pulls import DataPull
from lochness.models.data_push import DataPush
from lochness.models.data_sinks import DataSink
from lochness.sources.xnat.models.data_source import XnatDataSource

MODULE_NAME = "lochness.sources.xnat.tasks.pull_data"

console = utils.get_console()

logger = logging.getLogger(MODULE_NAME)
logargs: Dict[str, Any] = {
    "level": logging.DEBUG,
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)


def get_xnat_cred(xnat_data_source: XnatDataSource) -> Dict[str, str]:
    """Get XNAT credentials from the keystore."""
    config_file = utils.get_config_file_path()
    encryption_passphrase = config.parse(config_file, "general")[
        "encryption_passphrase"
    ]

    keystore = KeyStore.get_by_name_and_project(
        config_file,
        xnat_data_source.data_source_metadata.keystore_name,
        xnat_data_source.project_id,
        encryption_passphrase,
    )
    if keystore:
        import json
        return json.loads(keystore.key_value)
    else:
        raise ValueError("XNAT credentials not found in keystore")


def fetch_subject_data(
    xnat_data_source: XnatDataSource,
    subject_id: str,
    encryption_passphrase: str,
    timeout_s: int = 60,
) -> Optional[bytes]:
    """
    Fetches data for a single subject from XNAT.

    Args:
        xnat_data_source (XnatDataSource): The XNAT data source.
        subject_id (str): The subject ID to fetch data for.
        encryption_passphrase (str): The encryption passphrase for keystore access.
        timeout_s (int): Timeout for the API request.

    Returns:
        Optional[bytes]: The raw data from XNAT, or None if fetching fails.
    """
    project_id = xnat_data_source.project_id
    site_id = xnat_data_source.site_id
    data_source_name = xnat_data_source.data_source_name

    identifier = f"{project_id}::{site_id}::{data_source_name}::{subject_id}"
    logger.info(f"Fetching data for {identifier}...")

    try:
        # Get XNAT credentials
        credentials = get_xnat_cred(xnat_data_source)
        endpoint_url = xnat_data_source.data_source_metadata.endpoint_url
        api_token = credentials.get("api_token")

        # Connect to XNAT
        with xnat.connect(endpoint_url, user=api_token, password=api_token) as connection:
            # Get subject data
            subject = connection.projects[project_id].subjects[subject_id]
            
            # Get all experiments for this subject
            experiments = list(subject.experiments.keys())
            
            if not experiments:
                logger.warning(f"No experiments found for subject {subject_id}")
                return None

            # For now, get the first experiment (you might want to iterate through all)
            experiment_id = experiments[0]
            experiment = subject.experiments[experiment_id]
            
            # Download the experiment as a ZIP file
            # This will download all scans and resources for the experiment
            logger.info(f"Downloading experiment {experiment_id} for subject {subject_id}")
            
            # Create a temporary directory for the download
            import tempfile
            import zipfile
            import io
            
            with tempfile.TemporaryDirectory() as temp_dir:
                # Download the experiment to the temp directory
                downloaded_path = experiment.download(temp_dir)
                downloaded_path = Path(downloaded_path)
                
                # Create a ZIP file in memory
                zip_buffer = io.BytesIO()
                with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
                    # Add all files from the downloaded directory to the ZIP
                    for file_path in downloaded_path.rglob('*'):
                        if file_path.is_file():
                            # Add file to ZIP with relative path
                            arcname = file_path.relative_to(downloaded_path)
                            zip_file.write(file_path, arcname)
                
                # Return the ZIP data
                zip_buffer.seek(0)
                return zip_buffer.getvalue()

    except Exception as e:
        logger.error(f"Failed to fetch data for {identifier}: {e}")
        Logs(
            log_level="ERROR",
            log_message={
                "event": "xnat_data_pull_fetch_failed",
                "message": f"Failed to fetch data for {identifier}.",
                "project_id": project_id,
                "site_id": site_id,
                "data_source_name": data_source_name,
                "subject_id": subject_id,
                "error": str(e),
            },
        ).insert(config_file)
        return None


def save_subject_data(
    data: bytes,
    project_id: str,
    site_id: str,
    subject_id: str,
    data_source_name: str,
    config_file: Path,
) -> Optional[tuple[Path, str]]:
    """
    Saves the fetched subject data to the file system and records it in the database.

    Args:
        data (bytes): The raw data to save.
        project_id (str): The project ID.
        site_id (str): The site ID.
        subject_id (str): The subject ID.
        data_source_name (str): The name of the data source.
        config_file (Path): Path to the config file.

    Returns:
        Optional[Path]: The path to the saved file, or None if saving fails.
    """
    try:
        # Define the path where the data will be stored
        # Example: <lochness_root>/data/<project_id>/<site_id>/<data_source_name>/<subject_id>/<timestamp>.json
        lochness_root = config.parse(config_file, 'general')['lochness_root']
        output_dir = Path(lochness_root) / "data" / project_id / site_id / data_source_name / subject_id
        output_dir.mkdir(parents=True, exist_ok=True)

        timestamp = utils.get_timestamp()
        file_name = f"{timestamp}.zip"  # ZIP format for XNAT data
        file_path = output_dir / file_name

        with open(file_path, "wb") as f:
            f.write(data)

        # Record the file in the database
        file_model = File(
            file_path=file_path,
        )
        file_md5 = file_model.md5
        # Insert file_model into the database
        db.execute_queries(config_file, [file_model.to_sql_query()], show_commands=False)

        Logs(
            log_level="INFO",
            log_message={
                "event": "xnat_data_pull_save_success",
                "message": f"Successfully saved data for {subject_id} to {file_path}.",
                "project_id": project_id,
                "site_id": site_id,
                "data_source_name": data_source_name,
                "subject_id": subject_id,
                "file_path": str(file_path),
                "file_md5": file_md5 if file_md5 else None,
            },
        ).insert(config_file)
        return file_path, file_md5 if file_md5 else ""

    except Exception as e:
        logger.error(f"Failed to save data for {subject_id}: {e}")
        Logs(
            log_level="ERROR",
            log_message={
                "event": "xnat_data_pull_save_failed",
                "message": f"Failed to save data for {subject_id}.",
                "project_id": project_id,
                "site_id": site_id,
                "data_source_name": data_source_name,
                "subject_id": subject_id,
                "error": str(e),
            },
        ).insert(config_file)
        return None


def push_to_data_sink(
    file_path: Path,
    file_md5: str,
    project_id: str,
    site_id: str,
    config_file: Path,
) -> Optional[DataPush]:
    """
    Pushes a file to a data sink for the given project and site.

    Args:
        file_path (Path): Path to the file to push.
        file_md5 (str): MD5 hash of the file.
        project_id (str): The project ID.
        site_id (str): The site ID.
        config_file (Path): Path to the config file.

    Returns:
        Optional[DataPush]: The data push record if successful, None otherwise.
    """
    try:
        # Get data sinks for this project and site
        sql_query = f"""
            SELECT data_sink_id, data_sink_name, data_sink_metadata
            FROM data_sinks
            WHERE project_id = '{project_id}' AND site_id = '{site_id}'
        """
        df = db.execute_sql(config_file, sql_query)
        
        if df.empty:
            logger.warning(f"No data sinks found for {project_id}::{site_id}")
            return None

        # For now, use the first data sink found
        data_sink = df.iloc[0]
        data_sink_id = data_sink['data_sink_id']
        data_sink_name = data_sink['data_sink_name']
        data_sink_metadata = data_sink['data_sink_metadata']

        logger.info(f"Pushing {file_path} to data sink {data_sink_name}...")

        # Check if this is a MinIO data sink
        if data_sink_metadata.get('keystore_name'):
            # This is likely a MinIO data sink
            return push_to_minio_sink(
                file_path=file_path,
                file_md5=file_md5,
                data_sink_id=data_sink_id,
                data_sink_name=data_sink_name,
                data_sink_metadata=data_sink_metadata,
                project_id=project_id,
                site_id=site_id,
                config_file=config_file,
            )
        else:
            # For other data sink types, simulate upload for now
            start_time = datetime.now()
            import time
            time.sleep(1)  # Simulate upload time
            end_time = datetime.now()
            push_time_s = int((end_time - start_time).total_seconds())

            data_push = DataPush(
                data_sink_id=data_sink_id,
                file_path=str(file_path),
                file_md5=file_md5,
                push_time_s=push_time_s,
                push_metadata={
                    "data_sink_name": data_sink_name,
                    "data_sink_metadata": data_sink_metadata,
                    "upload_simulated": True,  # Flag to indicate this was simulated
                },
                push_timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            )

            # Insert the data push record
            db.execute_queries(config_file, [data_push.to_sql_query()], show_commands=False)

            logger.info(f"Successfully pushed {file_path} to data sink {data_sink_name}")
            Logs(
                log_level="INFO",
                log_message={
                    "event": "xnat_data_push_success",
                    "message": f"Successfully pushed {file_path} to data sink {data_sink_name}.",
                    "project_id": project_id,
                    "site_id": site_id,
                    "data_sink_name": data_sink_name,
                    "file_path": str(file_path),
                    "push_time_s": push_time_s,
                },
            ).insert(config_file)

            return data_push

    except Exception as e:
        logger.error(f"Failed to push {file_path} to data sink: {e}")
        Logs(
            log_level="ERROR",
            log_message={
                "event": "xnat_data_push_failed",
                "message": f"Failed to push {file_path} to data sink.",
                "project_id": project_id,
                "site_id": site_id,
                "file_path": str(file_path),
                "error": str(e),
            },
        ).insert(config_file)
        return None


def push_to_minio_sink(
    file_path: Path,
    file_md5: str,
    data_sink_id: int,
    data_sink_name: str,
    data_sink_metadata: Dict[str, Any],
    project_id: str,
    site_id: str,
    config_file: Path,
) -> Optional[DataPush]:
    """
    Pushes a file to a MinIO data sink.
    """
    try:
        # --- NEW LOGIC: Check for existing push with same md5 ---
        check_push_query = f"""
            SELECT 1 FROM data_push
            WHERE data_sink_id = {data_sink_id}
              AND file_path = '{str(file_path).replace("'", "''")}'
              AND file_md5 = '{file_md5}'
            LIMIT 1;
        """
        push_exists = db.execute_sql(config_file, check_push_query)
        if not push_exists.empty:
            logger.info(f"File {file_path} (md5={file_md5}) already pushed to MinIO sink {data_sink_name}, skipping.")
            Logs(
                log_level="INFO",
                log_message={
                    "event": "minio_data_push_already_exists",
                    "message": f"File {file_path} (md5={file_md5}) already pushed to MinIO sink {data_sink_name}, skipping.",
                    "file_path": str(file_path),
                    "data_sink_name": data_sink_name,
                    "project_id": project_id,
                    "site_id": site_id,
                },
            ).insert(config_file)
            return None
        # --- END NEW LOGIC ---
        # Extract MinIO configuration from data sink metadata
        bucket_name = data_sink_metadata.get('bucket')
        keystore_name = data_sink_metadata.get('keystore_name')
        endpoint_url = data_sink_metadata.get('endpoint')

        if not all([bucket_name, keystore_name, endpoint_url]):
            logger.error(f"Missing required MinIO configuration in data sink metadata: {data_sink_metadata}")
            return None

        # Import MinIO client
        from minio import Minio
        from minio.error import S3Error
        from lochness.sources.minio.tasks.credentials import get_minio_cred

        # Get MinIO credentials from keystore
        minio_creds = get_minio_cred(keystore_name, project_id)
        access_key = minio_creds["access_key"]
        secret_key = minio_creds["secret_key"]

        # Initialize MinIO client
        endpoint_host = endpoint_url.replace("http://", "").replace("https://", "")
        client = Minio(
            endpoint_host,
            access_key=access_key,
            secret_key=secret_key,
            secure=endpoint_url.startswith("https"),
        )

        # Ensure bucket exists
        if not client.bucket_exists(bucket_name):
            logger.info(f"Bucket '{bucket_name}' does not exist. Creating...")
            client.make_bucket(bucket_name)
            logger.info(f"Bucket '{bucket_name}' created.")
        else:
            logger.info(f"Bucket '{bucket_name}' already exists.")

        # Create object name based on file path structure
        # Extract the relative path from the lochness data directory
        lochness_root = config.parse(config_file, 'general')['lochness_root']
        relative_path = file_path.relative_to(Path(lochness_root) / "data")
        object_name = str(relative_path).replace("\\", "/")  # Ensure forward slashes for S3

        # Upload the file
        logger.info(f"Uploading '{file_path}' to '{bucket_name}/{object_name}'...")
        start_time = datetime.now()
        
        client.fput_object(
            bucket_name,
            object_name,
            str(file_path),
            content_type="application/zip",
        )
        
        end_time = datetime.now()
        push_time_s = int((end_time - start_time).total_seconds())
        logger.info(f"Upload successful. Time taken: {push_time_s} seconds.")

        # Create data push record
        data_push = DataPush(
            data_sink_id=data_sink_id,
            file_path=str(file_path),
            file_md5=file_md5,
            push_time_s=push_time_s,
            push_metadata={
                "object_name": object_name,
                "bucket_name": bucket_name,
                "endpoint_url": endpoint_url,
                "upload_simulated": False,  # Flag to indicate this was real upload
            },
            push_timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        )

        # Insert the data push record
        db.execute_queries(config_file, [data_push.to_sql_query()], show_commands=False)

        logger.info(f"Successfully pushed {file_path} to MinIO data sink {data_sink_name}")
        Logs(
            log_level="INFO",
            log_message={
                "event": "xnat_data_push_success",
                "message": f"Successfully pushed {file_path} to MinIO data sink {data_sink_name}.",
                "project_id": project_id,
                "site_id": site_id,
                "data_sink_name": data_sink_name,
                "file_path": str(file_path),
                "object_name": object_name,
                "bucket_name": bucket_name,
                "push_time_s": push_time_s,
            },
        ).insert(config_file)

        return data_push

    except S3Error as e:
        logger.error(f"MinIO S3 Error while pushing {file_path}: {e}")
        Logs(
            log_level="ERROR",
            log_message={
                "event": "xnat_data_push_minio_error",
                "message": f"MinIO S3 Error while pushing {file_path}.",
                "project_id": project_id,
                "site_id": site_id,
                "file_path": str(file_path),
                "error": str(e),
            },
        ).insert(config_file)
        return None
    except Exception as e:
        logger.error(f"Failed to push {file_path} to MinIO data sink: {e}")
        Logs(
            log_level="ERROR",
            log_message={
                "event": "xnat_data_push_failed",
                "message": f"Failed to push {file_path} to MinIO data sink.",
                "project_id": project_id,
                "site_id": site_id,
                "file_path": str(file_path),
                "error": str(e),
            },
        ).insert(config_file)
        return None


def pull_all_data(config_file: Path, project_id: str = None, site_id: str = None, push_to_sink: bool = False, force_download: bool = False):
    """
    Main function to pull data for all active XNAT data sources and subjects.
    """
    Logs(
        log_level="INFO",
        log_message={
            "event": "xnat_data_pull_start",
            "message": "Starting XNAT data pull process.",
            "project_id": project_id,
            "site_id": site_id,
            "push_to_sink": push_to_sink,
            "force_download": force_download,
        },
    ).insert(config_file)

    encryption_passphrase = config.parse(config_file, 'general')['encryption_passphrase']

    active_xnat_data_sources = XnatDataSource.get_all_xnat_data_sources(
        config_file=config_file,
        encryption_passphrase=encryption_passphrase,
        active_only=True
    )

    if project_id:
        active_xnat_data_sources = [ds for ds in active_xnat_data_sources if ds.project_id == project_id]
    if site_id:
        active_xnat_data_sources = [ds for ds in active_xnat_data_sources if ds.site_id == site_id]

    if not active_xnat_data_sources:
        logger.info("No active XNAT data sources found for data pull.")
        Logs(
            log_level="INFO",
            log_message={
                "event": "xnat_data_pull_no_active_sources",
                "message": "No active XNAT data sources found for data pull.",
                "project_id": project_id,
                "site_id": site_id,
            },
        ).insert(config_file)
        return

    logger.info(f"Found {len(active_xnat_data_sources)} active XNAT data sources for data pull.")
    Logs(
        log_level="INFO",
        log_message={
            "event": "xnat_data_pull_active_sources_found",
            "message": f"Found {len(active_xnat_data_sources)} active XNAT data sources for data pull.",
            "count": len(active_xnat_data_sources),
            "project_id": project_id,
            "site_id": site_id,
        },
    ).insert(config_file)

    for xnat_data_source in active_xnat_data_sources:
        # Get subjects for this data source
        subjects_in_db = Subject.get_subjects_for_project_site(
            project_id=xnat_data_source.project_id,
            site_id=xnat_data_source.site_id,
            config_file=config_file
        )

        if not subjects_in_db:
            logger.info(f"No subjects found for {xnat_data_source.project_id}::{xnat_data_source.site_id}.")
            Logs(
                log_level="INFO",
                log_message={
                    "event": "xnat_data_pull_no_subjects",
                    "message": f"No subjects found for {xnat_data_source.project_id}::{xnat_data_source.site_id}.",
                    "project_id": xnat_data_source.project_id,
                    "site_id": xnat_data_source.site_id,
                    "data_source_name": xnat_data_source.data_source_name,
                },
            ).insert(config_file)
            continue

        logger.info(f"Found {len(subjects_in_db)} subjects for {xnat_data_source.data_source_name}.")
        Logs(
            log_level="INFO",
            log_message={
                "event": "xnat_data_pull_subjects_found",
                "message": f"Found {len(subjects_in_db)} subjects for {xnat_data_source.data_source_name}.",
                "count": len(subjects_in_db),
                "project_id": xnat_data_source.project_id,
                "site_id": xnat_data_source.site_id,
                "data_source_name": xnat_data_source.data_source_name,
            },
        ).insert(config_file)

        for subject in subjects_in_db:
            if not force_download:
                # --- Check if file exists for this subject/data source ---
                lochness_root = config.parse(config_file, 'general')['lochness_root']
                subject_dir = Path(lochness_root) / "data" / subject.project_id / subject.site_id / xnat_data_source.data_source_name / subject.subject_id
                check_file_query = f"""
                    SELECT file_path FROM files
                    WHERE file_path LIKE '{str(subject_dir).replace("'", "''")}/%'
                    LIMIT 1;
                """
                file_exists = db.execute_sql(config_file, check_file_query)
                if not file_exists.empty:
                    logger.info(f"File(s) already exist for subject {subject.subject_id} in {subject_dir}, skipping download.")
                    Logs(
                        log_level="INFO",
                        log_message={
                            "event": "xnat_data_pull_already_exists",
                            "message": f"File(s) already exist for subject {subject.subject_id} in {subject_dir}, skipping download.",
                            "subject_id": subject.subject_id,
                            "project_id": subject.project_id,
                            "site_id": subject.site_id,
                            "data_source_name": xnat_data_source.data_source_name,
                            "subject_dir": str(subject_dir),
                        },
                    ).insert(config_file)
                    continue
            start_time = datetime.now()
            raw_data = fetch_subject_data(
                xnat_data_source=xnat_data_source,
                subject_id=subject.subject_id,
                encryption_passphrase=encryption_passphrase,
            )

            if raw_data:
                result = save_subject_data(
                    data=raw_data,
                    project_id=subject.project_id,
                    site_id=subject.site_id,
                    subject_id=subject.subject_id,
                    data_source_name=xnat_data_source.data_source_name,
                    config_file=config_file,
                )
                if result:
                    file_path, file_md5 = result
                    end_time = datetime.now()
                    pull_time_s = int((end_time - start_time).total_seconds())

                    lochness_root_str = str(config.parse(config_file, 'general')['lochness_root'])
                    data_pull = DataPull(
                        subject_id=subject.subject_id,
                        data_source_name=xnat_data_source.data_source_name,
                        site_id=subject.site_id,
                        project_id=subject.project_id,
                        file_path=str(file_path),
                        file_md5=file_md5,
                        pull_time_s=pull_time_s,
                        pull_metadata={
                            "xnat_endpoint": xnat_data_source.data_source_metadata.endpoint_url,
                            "records_pulled_bytes": len(raw_data),
                            "relative_path": str(file_path.relative_to(lochness_root_str)),
                        },
                    )
                    db.execute_queries(config_file, [data_pull.to_sql_query()], show_commands=False)

                    # Push to data sink if requested
                    if push_to_sink:
                        push_to_data_sink(
                            file_path=file_path,
                            file_md5=file_md5,
                            project_id=subject.project_id,
                            site_id=subject.site_id,
                            config_file=config_file,
                        )
            break # Stop after the first subject

    Logs(
        log_level="INFO",
        log_message={
            "event": "xnat_data_pull_complete",
            "message": "Finished XNAT data pull process.",
            "project_id": project_id,
            "site_id": site_id,
            "push_to_sink": push_to_sink,
        },
    ).insert(config_file)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pull XNAT data for all or specific project/site.")
    parser.add_argument('--project_id', type=str, default=None, help='Project ID to pull data for (optional)')
    parser.add_argument('--site_id', type=str, default=None, help='Site ID to pull data for (optional)')
    parser.add_argument('--push_to_sink', action='store_true', help='Push pulled files to data sink')
    parser.add_argument('--force_download', action='store_true', help='Force download from XNAT even if files exist')
    args = parser.parse_args()

    config_file = Path(__file__).resolve().parents[4] / "sample.config.ini"
    print(f"Resolved config_file path: {config_file}") # Debugging line
    logs.configure_logging(
        config_file=config_file, module_name=MODULE_NAME, logger=logger
    )

    logger.info("Starting XNAT data pull...")

    if not config_file.exists():
        logger.error(f"Config file does not exist: {config_file}")
        Logs(
            log_level="FATAL",
            log_message={
                "event": "xnat_data_pull_config_missing",
                "message": f"Config file does not exist: {config_file}",
                "config_file_path": str(config_file),
            },
        ).insert(config_file)
        sys.exit(1)

    pull_all_data(config_file=config_file, project_id=args.project_id, site_id=args.site_id, push_to_sink=args.push_to_sink, force_download=args.force_download)

    logger.info("Finished XNAT data pull.") 