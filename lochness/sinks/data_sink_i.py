"""
Defines the abstract base class for data sinks.

This class serves as a contract for all data sink implementations,
ensuring they provide the necessary methods for
pushing data.
"""

import abc
from typing import Dict, Optional

from pathlib import Path

from lochness.models.data_sinks import DataSink
from lochness.models.data_push import DataPush


class DataSinkI(abc.ABC):
    """
    An abstract base class for data sinks.

    It defines a contract that all concrete data sinks must follow.
    """

    data_sink: DataSink

    def __init__(self, data_sink: DataSink):
        self.data_sink = data_sink

    @abc.abstractmethod
    def push(
        self,
        file_to_push: Path,
        push_metadata: Dict[str, str],
        config_file: Path,
    ) -> Optional[DataPush]:
        """
        Pushes data to the sink.

        This is an abstract method and MUST be implemented by any subclass.
        """

    @abc.abstractmethod
    def pull(
        self,
        data_push: DataPush,
        destination_path: Path,
        config_file: Path,
    ) -> Optional[Path]:
        """
        Pulls data from the sink.

        This is an abstract method and MUST be implemented by any subclass.
        """

    def __str__(self):
        return f"<{self.__class__.__name__}>"
