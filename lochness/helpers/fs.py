"""
Module for file system operations.
"""
import logging
import os
import shutil
from contextlib import contextmanager
from pathlib import Path
from typing import IO, Any, Generator, List

from lochness.helpers import cli

logger = logging.getLogger(__name__)


def chown(file_path: Path, user: str, group: str) -> None:
    """
    Changes the ownership of a file.

    Args:
        file_path (Path): The path to the file.
        user (str): The user to change the ownership to.
        group (str): The group to change the ownership to.

    Returns:
        None
    """
    command_array = ["chown", "-R", f"{user}:{group}", str(file_path)]
    cli.execute_commands(
        command_array,
        shell=True,
        on_fail=lambda: logger.error("Failed to change ownership."),
    )


def chmod(file_path: Path, mode: int) -> None:
    """
    Changes the permissions of a file.

    Args:
        file_path (Path): The path to the file.
        mode (int): the mode to change the permissions to.

    Returns:
        None
    """
    command_array: List[str] = ["chmod", "-R", str(mode), str(file_path)]
    cli.execute_commands(
        command_array,
        shell=True,
        on_fail=lambda: logger.error("Failed to change permissions."),
    )


def remove_directory(path: Path) -> None:
    """
    Remove all files in the specified directory.

    Args:
        path (str): The path to the directory to be cleared.

    Returns:
        None
    """

    # Check if directory exists
    if not Path(path).is_dir():
        return

    shutil.rmtree(path)


def remove(path: Path) -> None:
    """
    Remove a file or directory. Aso removes parent directories if they are empty.

    Args:
        path (Path): The path to the file or directory to remove.

    Returns:
        None
    """
    if path.is_dir():
        remove_directory(path)
    else:
        path.unlink()

    # Remove parent directories if they are empty
    parent = path.parent
    while parent != Path("/") and not any(parent.iterdir()):
        remove_directory(parent)
        parent = parent.parent


@contextmanager
def atomic_write(
    file_path: Path, mode: str = "w", **kwargs: Any
) -> Generator[IO, None, None]:
    """
    Write to a file atomically.

    Content is written to a hidden temporary file in the same directory and
    renamed onto file_path only once the write succeeds, so other processes
    never see a partially written file.

    Args:
        file_path (Path): The final path of the file.
        mode (str): The mode to open the temporary file with.
        **kwargs: Additional arguments passed to open().

    Yields:
        IO: The open temporary file.
    """
    file_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = file_path.with_name(f".{file_path.name}.{os.getpid()}.part")
    try:
        with open(temp_path, mode, **kwargs) as f:
            yield f
        os.replace(temp_path, file_path)
    finally:
        temp_path.unlink(missing_ok=True)


def copy(source: Path, destination: Path) -> None:
    """
    Copy a file or directory to a new location.

    Args:
        source (Path): The source file or directory to copy.
        destination (Path): The destination file or directory to copy to.

    Returns:
        None
    """
    if source.is_dir():
        shutil.copytree(source, destination)
    else:
        shutil.copy2(source, destination)


def create_link(source: Path, destination: Path, softlink: bool = True) -> None:
    """
    Create a link from the source to the destination.

    Note:
    - Both source and destination must be on the same filesystem.
    - The destination must not already exist.

    Args:
        source (Path): The source of the symbolic link.
        destination (Path): The destination of the symbolic link.
        softlink (bool, optional): Whether to create a soft link.
            Defaults to True. If False, a hard link is created.

    Returns:
        None
    """
    if not source.exists():
        logger.error(f"Source path does not exist: {source}")
        raise FileNotFoundError

    if destination.exists():
        logger.error(f"Destination path already exists: {destination}")
        raise FileExistsError

    if softlink:
        logger.debug(f"Creating soft link from {source} to {destination}")
        destination.symlink_to(source)
    else:
        logger.debug(f"Creating hard link from {source} to {destination}")
        destination.hardlink_to(source)
