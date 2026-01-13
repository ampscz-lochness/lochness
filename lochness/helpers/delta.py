"""
Delta module for computing and applying file deltas.

This module provides functionality to:
- Calculate deltas between two file versions
- Apply deltas to reconstruct files
- Traverse delta chains to arrive at the final file version

The deltas are stored in file_metadata with 'parent_md5' and 'delta' keys,
allowing reconstruction by traversing the chain from a template file.
"""

import base64
import zlib
from pathlib import Path
from typing import Optional, Tuple, List, Dict, Any

from diff_match_patch import diff_match_patch

from lochness.helpers import db
from lochness.helpers.hash import compute_fingerprint


class DeltaError(Exception):
    """Base exception for delta operations."""


class DeltaPatchError(DeltaError):
    """Raised when applying a delta patch fails."""


class DeltaChainError(DeltaError):
    """Raised when delta chain resolution fails."""


def compute_delta(
    original_content: str,
    modified_content: str,
    human_readable: bool = False,
) -> str:
    """
    Compute the delta (patch) between two text contents.

    Args:
        original_content: The original text content (v0).
        modified_content: The modified text content (v1).
        human_readable: If True, returns a human-readable unified diff format.
            If False, returns a compressed binary delta (smaller).

    Returns:
        The delta as a string. If human_readable=False, the delta is
        base64-encoded compressed binary data.
    """
    dmp = diff_match_patch()

    # Compute the diff
    diffs = dmp.diff_main(original_content, modified_content)
    dmp.diff_cleanupEfficiency(diffs)

    # Create compact binary delta
    patches = dmp.patch_make(original_content, diffs)
    patch_text = dmp.patch_toText(patches)

    if human_readable:
        return patch_text

    # Compress and encode for storage
    compressed = zlib.compress(patch_text.encode("utf-8"), level=9)
    return base64.b64encode(compressed).decode("ascii")


def compute_delta_from_files(
    original_path: Path,
    modified_path: Path,
    human_readable: bool = False,
    encoding: str = "utf-8",
) -> Tuple[str, str, str]:
    """
    Compute the delta between two files.

    Args:
        original_path: Path to the original file.
        modified_path: Path to the modified file.
        human_readable: If True, returns human-readable delta format.
        encoding: File encoding to use when reading files.

    Returns:
        A tuple of (delta, original_md5, modified_md5).

    Raises:
        FileNotFoundError: If either file does not exist.
    """
    if not original_path.exists():
        raise FileNotFoundError(f"Original file not found: {original_path}")
    if not modified_path.exists():
        raise FileNotFoundError(f"Modified file not found: {modified_path}")

    original_content = original_path.read_text(encoding=encoding)
    modified_content = modified_path.read_text(encoding=encoding)

    original_md5 = compute_fingerprint(original_path)
    modified_md5 = compute_fingerprint(modified_path)

    delta = compute_delta(original_content, modified_content, human_readable)

    return delta, original_md5, modified_md5


def apply_delta(
    original_content: str,
    delta: str,
    human_readable: bool = False,
) -> str:
    """
    Apply a delta patch to original content to produce the modified content.

    Args:
        original_content: The original text content.
        delta: The delta/patch to apply.
        human_readable: If True, delta is in human-readable format.
            If False, delta is base64-encoded compressed data.

    Returns:
        The resulting text after applying the delta.

    Raises:
        DeltaPatchError: If the patch cannot be applied cleanly.
    """
    dmp = diff_match_patch()

    if human_readable:
        patch_text = delta
    else:
        # Decode and decompress
        try:
            compressed = base64.b64decode(delta.encode("ascii"))
            patch_text = zlib.decompress(compressed).decode("utf-8")
        except (ValueError, zlib.error) as e:
            raise DeltaPatchError(f"Failed to decode delta: {e}") from e

    # Parse and apply patches
    try:
        patches = dmp.patch_fromText(patch_text)
    except ValueError as e:
        raise DeltaPatchError(f"Invalid patch format: {e}") from e

    result, success_flags = dmp.patch_apply(patches, original_content)

    # Check if all patches applied successfully
    if not all(success_flags):
        failed_count = success_flags.count(False)
        total_count = len(success_flags)
        raise DeltaPatchError(
            f"Failed to apply {failed_count}/{total_count} patch hunks. "
            "The original content may not match the expected base."
        )

    return result


def apply_delta_to_file(
    original_path: Path,
    delta: str,
    output_path: Optional[Path] = None,
    human_readable: bool = False,
    encoding: str = "utf-8",
) -> Path:
    """
    Apply a delta patch to a file.

    Args:
        original_path: Path to the original file.
        delta: The delta/patch to apply.
        output_path: Path for the output file. If None, overwrites original.
        human_readable: If True, delta is in human-readable format.
        encoding: File encoding to use.

    Returns:
        Path to the output file.

    Raises:
        FileNotFoundError: If the original file does not exist.
        DeltaPatchError: If the patch cannot be applied.
    """
    if not original_path.exists():
        raise FileNotFoundError(f"Original file not found: {original_path}")

    original_content = original_path.read_text(encoding=encoding)
    result = apply_delta(original_content, delta, human_readable)

    target_path = output_path if output_path else original_path
    target_path.write_text(result, encoding=encoding)

    return target_path


def apply_delta_chain(
    base_content: str,
    deltas: List[str],
    human_readable: bool = False,
) -> str:
    """
    Apply a chain of deltas sequentially to arrive at the final content.

    Args:
        base_content: The original/template content (v0).
        deltas: List of deltas to apply in order [delta_0->1, delta_1->2, ...].
        human_readable: If True, deltas are in human-readable format.

    Returns:
        The final content after applying all deltas.

    Raises:
        DeltaPatchError: If any delta in the chain fails to apply.
    """
    current_content = base_content

    for i, delta in enumerate(deltas):
        try:
            current_content = apply_delta(current_content, delta, human_readable)
        except DeltaPatchError as e:
            raise DeltaPatchError(
                f"Failed to apply delta {i + 1}/{len(deltas)}: {e}"
            ) from e

    return current_content


def create_file_metadata_with_delta(
    parent_md5: str,
    delta: str,
    human_readable: bool = False,
    additional_metadata: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Create file_metadata dictionary containing delta information.

    Args:
        parent_md5: The MD5/fingerprint of the parent file.
        delta: The delta from parent to this file.
        human_readable: Whether the delta is in human-readable format.
        additional_metadata: Additional metadata to include.

    Returns:
        A dictionary suitable for storing in file_metadata.
    """
    metadata: Dict[str, Any] = {
        "parent_md5": parent_md5,
        "delta": delta,
        "delta_format": "human_readable" if human_readable else "compressed",
    }

    if additional_metadata:
        metadata.update(additional_metadata)

    return metadata


def resolve_delta_chain_from_db(
    config_file: Path,
    target_md5: str,
    max_chain_length: int = 100,
) -> Tuple[str, List[Dict[str, Any]]]:
    """
    Resolve the delta chain from the database, starting from target_md5
    and walking back to the root template file.

    Args:
        config_file: Path to the database configuration file.
        target_md5: The MD5 of the target file version.
        max_chain_length: Maximum chain length to prevent infinite loops.

    Returns:
        A tuple of (root_md5, chain) where chain is a list of metadata dicts
        in order from root to target (i.e., deltas to apply sequentially).

    Raises:
        DeltaChainError: If the chain cannot be resolved.
    """
    chain: List[Dict[str, Any]] = []
    current_md5 = target_md5
    visited: set = set()

    while len(chain) < max_chain_length:
        if current_md5 in visited:
            raise DeltaChainError(
                f"Circular reference detected in delta chain at MD5: {current_md5}"
            )
        visited.add(current_md5)

        # Query for the file with this MD5
        query = f"""
        SELECT file_path, file_md5, file_metadata
        FROM files
        WHERE file_md5 = '{db.sanitize_string(current_md5)}'
        LIMIT 1;
        """

        result_df = db.execute_sql(config_file, query)

        if result_df.empty:
            raise DeltaChainError(f"File with MD5 {current_md5} not found in database")

        row = result_df.iloc[0]
        metadata = row.get("file_metadata") or {}

        parent_md5 = metadata.get("parent_md5")

        if parent_md5 is None:
            # This is the root/template file
            return current_md5, list(reversed(chain))

        # Add to chain and continue traversing
        chain.append(
            {
                "file_md5": current_md5,
                "file_path": row["file_path"],
                "delta": metadata.get("delta"),
                "delta_format": metadata.get("delta_format", "compressed"),
            }
        )
        current_md5 = parent_md5

    raise DeltaChainError(
        f"Delta chain exceeded maximum length of {max_chain_length}. "
        "Possible infinite loop or very long chain."
    )


def reconstruct_file_from_chain(
    config_file: Path,
    target_md5: str,
    template_content: str,
    max_chain_length: int = 100,
) -> str:
    """
    Reconstruct file content by resolving and applying the delta chain.

    Args:
        config_file: Path to the database configuration file.
        target_md5: The MD5 of the target file version to reconstruct.
        template_content: The content of the root/template file.
        max_chain_length: Maximum chain length to prevent infinite loops.

    Returns:
        The reconstructed file content.

    Raises:
        DeltaChainError: If the chain cannot be resolved.
        DeltaPatchError: If any delta fails to apply.
    """
    _, chain = resolve_delta_chain_from_db(
        config_file, target_md5, max_chain_length
    )

    if not chain:
        # Target is the root file itself
        return template_content

    # Extract deltas and their formats
    current_content = template_content

    for i, link in enumerate(chain):
        delta = link.get("delta")
        if delta is None:
            raise DeltaChainError(
                f"Missing delta in chain at position {i + 1}, "
                f"file_md5: {link['file_md5']}"
            )

        human_readable = link.get("delta_format") == "human_readable"

        try:
            current_content = apply_delta(current_content, delta, human_readable)
        except DeltaPatchError as e:
            raise DeltaPatchError(
                f"Failed to apply delta at chain position {i + 1} "
                f"(file_md5: {link['file_md5']}): {e}"
            ) from e

    return current_content


def get_delta_chain_info(
    config_file: Path,
    target_md5: str,
    max_chain_length: int = 100,
) -> Dict[str, Any]:
    """
    Get information about a delta chain without reconstructing the content.

    Args:
        config_file: Path to the database configuration file.
        target_md5: The MD5 of the target file version.
        max_chain_length: Maximum chain length.

    Returns:
        A dictionary containing chain information:
        - root_md5: The MD5 of the root/template file
        - chain_length: Number of deltas in the chain
        - chain: List of chain link information
        - total_delta_size: Total size of all deltas in bytes
    """
    root_md5, chain = resolve_delta_chain_from_db(
        config_file, target_md5, max_chain_length
    )

    total_delta_size = sum(len(link.get("delta", "").encode("utf-8")) for link in chain)

    return {
        "root_md5": root_md5,
        "chain_length": len(chain),
        "chain": chain,
        "total_delta_size": total_delta_size,
    }


def is_delta_efficient(
    original_content: str,
    modified_content: str,
    threshold: float = 0.5,
) -> bool:
    """
    Check if storing a delta is more efficient than storing the full file.

    Args:
        original_content: The original content.
        modified_content: The modified content.
        threshold: Ratio threshold. If delta_size / modified_size > threshold,
            storing the full file might be more efficient.

    Returns:
        True if delta storage is efficient, False otherwise.
    """
    delta = compute_delta(original_content, modified_content, human_readable=False)

    delta_size = len(delta.encode("utf-8"))
    modified_size = len(modified_content.encode("utf-8"))

    if modified_size == 0:
        return True

    return (delta_size / modified_size) <= threshold


def convert_delta_format(
    delta: str,
    to_human_readable: bool,
) -> str:
    """
    Convert a delta between human-readable and compressed formats.

    Args:
        delta: The delta to convert.
        to_human_readable: If True, convert to human-readable format.
            If False, convert to compressed format.

    Returns:
        The converted delta.
    """
    if to_human_readable:
        # Decompress to human-readable
        try:
            compressed = base64.b64decode(delta.encode("ascii"))
            return zlib.decompress(compressed).decode("utf-8")
        except (ValueError, zlib.error):
            # Already in human-readable format
            return delta
    else:
        # Compress from human-readable
        try:
            # Check if already compressed
            base64.b64decode(delta.encode("ascii"))
            return delta  # Already compressed
        except (ValueError, UnicodeDecodeError):
            # Compress
            compressed = zlib.compress(delta.encode("utf-8"), level=9)
            return base64.b64encode(compressed).decode("ascii")
