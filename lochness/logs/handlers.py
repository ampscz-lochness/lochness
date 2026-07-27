"""
Provides custom logging handlers for PostgreSQL.
"""

import logging
import queue
import threading
import time
from pathlib import Path
from typing import List, Literal

from lochness.helpers import db
from lochness.models import logs


class SynchronousPostgresLogHandler(logging.Handler):
    """
    A logging handler that inserts log records into a PostgreSQL database
    synchronously.

    This handler is intended for use in scenarios where immediate log
    insertion is required, at the cost of performance / latency.
    """

    def __init__(self, config_file: Path):
        super().__init__()
        self.config_file = config_file

    def emit(self, record: logging.LogRecord) -> None:
        level_map = {
            "DEBUG": "DEBUG",
            "INFO": "INFO",
            "WARNING": "WARN",
            "ERROR": "ERROR",
            "CRITICAL": "FATAL",
        }
        result = level_map.get(record.levelname, "DEBUG")
        log_level: Literal["DEBUG", "INFO", "WARN", "ERROR", "FATAL"] = result  # type: ignore

        log_entry = logs.Logs(
            log_level=log_level,
            log_message={
                "message": record.getMessage(),
                "module": record.module,
                "filename": record.filename,
                "lineno": record.lineno,
                "funcName": record.funcName,
            },
        )
        log_entry.insert(config_file=self.config_file)


class BatchedPostgresLogHandler(logging.Handler):
    """
    A logging handler that batches log records and inserts them into a PostgreSQL database.
    This handler uses a separate thread to process the log records in batches.
    """

    def __init__(
        self, config_file: Path, batch_size: int = 100, flush_interval_s: int = 5
    ):
        super().__init__()
        self.config_file = config_file
        self.batch_size = batch_size
        self.flush_interval_s = flush_interval_s
        self.log_queue: queue.Queue[logs.Logs] = queue.Queue()
        self.shutdown_event = threading.Event()
        self.worker = threading.Thread(target=self._process_queue, daemon=True)
        self.worker.start()

    def emit(self, record: logging.LogRecord) -> None:
        level_map = {
            "DEBUG": "DEBUG",
            "INFO": "INFO",
            "WARNING": "WARN",
            "ERROR": "ERROR",
            "CRITICAL": "FATAL",
        }
        result = level_map.get(record.levelname, "DEBUG")
        log_level: Literal["DEBUG", "INFO", "WARN", "ERROR", "FATAL"] = result  # type: ignore

        log_entry = logs.Logs(
            log_level=log_level,
            log_message={
                "message": record.getMessage(),
                "module": record.module,
                "filename": record.filename,
                "lineno": record.lineno,
                "funcName": record.funcName,
            }
        )
        self.log_queue.put(log_entry)

    def _process_queue(self) -> None:
        """
        Process the log queue in a separate thread.

        This method runs in a loop, checking for new log entries and flushing them
        to the database in batches.

        It will continue to run until the shutdown event is set and the queue is empty.
        """
        batch: List[logs.Logs] = []
        last_flush_time = time.time()
        while not self.shutdown_event.is_set() or not self.log_queue.empty():
            try:
                timeout = self.flush_interval_s - (time.time() - last_flush_time)
                timeout = max(timeout, 0)
                log_entry = self.log_queue.get(timeout=timeout)
                batch.append(log_entry)
                if len(batch) >= self.batch_size:
                    self._flush_batch(batch)
                    batch.clear()
                    last_flush_time = time.time()
            except queue.Empty:
                if batch and (time.time() - last_flush_time) >= self.flush_interval_s:
                    self._flush_batch(batch)
                    batch.clear()
                    last_flush_time = time.time()

        # Flush any remaining logs in the batch
        if batch:
            self._flush_batch(batch)
            batch.clear()

    def _flush_batch(self, batch: List[logs.Logs]) -> None:
        """
        Flush the batch of log entries to the database.

        Args:
            batch (List[logs.Logs]): The batch of log entries to flush.
        """
        sql_queries = [log_entry.to_sql_query() for log_entry in batch]
        db.execute_queries(  # type: ignore
            config_file=self.config_file,
            queries=sql_queries,
            show_commands=False,
            silent=True,
        )

    def close(self) -> None:
        """
        Close the handler and flush any remaining logs.
        """
        self.shutdown_event.set()
        self.worker.join(
            timeout=self.flush_interval_s * 1.5
        )  # Allow time for the worker to finish
        super().close()
