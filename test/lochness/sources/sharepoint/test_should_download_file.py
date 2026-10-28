"""
Unit tests for SharePoint download skip logic.

Files that were already pulled and pushed are removed locally by the post-push
cleanup sweep. These tests make sure such files are not re-downloaded on the
next pull, while new or updated files still are.
"""

import sys
from pathlib import Path
from unittest.mock import patch

file = Path(__file__).resolve()
root_dir = None
for parent in file.parents:
    if parent.name == "lochness_v2":
        root_dir = parent

sys.path.append(str(root_dir))

from lochness.models.data_pulls import DataPull
from lochness.sources.sharepoint import utils as sharepoint_utils

CONFIG_FILE = Path("/nonexistent/config.ini")
PROJECT_ID = "Procan"
SITE_ID = "BI"
REMOTE_HASH = "remote-quickxorhash=="
UTILS = "lochness.sources.sharepoint.utils"


def make_data_pull(file_path: Path, quick_xor_hash: str) -> DataPull:
    return DataPull(
        subject_id="BI11009",
        data_source_name="ProcanBI_sharepoint_eeg",
        site_id=SITE_ID,
        project_id=PROJECT_ID,
        file_path=str(file_path),
        file_md5="abc123",
        pull_time_s=10,
        pull_metadata={"quickxorhash": quick_xor_hash},
    )


def call_should_download(local_file_path: Path) -> bool:
    return sharepoint_utils.should_download_file(
        local_file_path,
        REMOTE_HASH,
        config_file=CONFIG_FILE,
        project_id=PROJECT_ID,
        site_id=SITE_ID,
    )


def test_skips_when_local_file_and_hash_match(tmp_path):
    local_file = tmp_path / "eeg.zip"
    local_file.write_bytes(b"data")
    (tmp_path / ".eeg.zip.quickxorhash").write_text(REMOTE_HASH)

    with patch(f"{UTILS}.DataPull.get_most_recent_data_pull_for_file_path") as lookup:
        assert call_should_download(local_file) is False
        lookup.assert_not_called()


def test_downloads_when_local_hash_differs(tmp_path):
    local_file = tmp_path / "eeg.zip"
    local_file.write_bytes(b"data")
    (tmp_path / ".eeg.zip.quickxorhash").write_text("old-hash")

    assert call_should_download(local_file) is True


def test_skips_when_cleaned_up_after_push(tmp_path):
    """Regression: files removed by the post-push cleanup were re-pulled daily."""
    local_file = tmp_path / "eeg.zip"

    with patch(
        f"{UTILS}.DataPull.get_most_recent_data_pull_for_file_path",
        return_value=make_data_pull(local_file, REMOTE_HASH),
    ), patch(
        f"{UTILS}.File.version_has_any_pushes", return_value=True
    ), patch(
        f"{UTILS}.File.version_has_pending_pushes", return_value=False
    ) as pending:
        assert call_should_download(local_file) is False
        pending.assert_called_once_with(
            config_file=CONFIG_FILE,
            file_path=local_file,
            file_md5="abc123",
            project_id=PROJECT_ID,
            site_id=SITE_ID,
        )


def test_downloads_when_never_pulled(tmp_path):
    local_file = tmp_path / "eeg.zip"

    with patch(
        f"{UTILS}.DataPull.get_most_recent_data_pull_for_file_path",
        return_value=None,
    ):
        assert call_should_download(local_file) is True


def test_downloads_when_remote_file_changed(tmp_path):
    local_file = tmp_path / "eeg.zip"

    with patch(
        f"{UTILS}.DataPull.get_most_recent_data_pull_for_file_path",
        return_value=make_data_pull(local_file, "old-hash"),
    ):
        assert call_should_download(local_file) is True


def test_downloads_when_never_pushed(tmp_path):
    local_file = tmp_path / "eeg.zip"

    with patch(
        f"{UTILS}.DataPull.get_most_recent_data_pull_for_file_path",
        return_value=make_data_pull(local_file, REMOTE_HASH),
    ), patch(f"{UTILS}.File.version_has_any_pushes", return_value=False):
        assert call_should_download(local_file) is True


def test_downloads_when_push_pending(tmp_path):
    local_file = tmp_path / "eeg.zip"

    with patch(
        f"{UTILS}.DataPull.get_most_recent_data_pull_for_file_path",
        return_value=make_data_pull(local_file, REMOTE_HASH),
    ), patch(
        f"{UTILS}.File.version_has_any_pushes", return_value=True
    ), patch(
        f"{UTILS}.File.version_has_pending_pushes", return_value=True
    ):
        assert call_should_download(local_file) is True


def test_downloads_when_remote_hash_missing(tmp_path):
    local_file = tmp_path / "eeg.zip"

    with patch(f"{UTILS}.DataPull.get_most_recent_data_pull_for_file_path") as lookup:
        assert (
            sharepoint_utils.should_download_file(
                local_file,
                None,
                config_file=CONFIG_FILE,
                project_id=PROJECT_ID,
                site_id=SITE_ID,
            )
            is True
        )
        lookup.assert_not_called()


def test_downloads_without_db_context(tmp_path):
    local_file = tmp_path / "eeg.zip"

    with patch(f"{UTILS}.DataPull.get_most_recent_data_pull_for_file_path") as lookup:
        assert sharepoint_utils.should_download_file(local_file, REMOTE_HASH) is True
        lookup.assert_not_called()


def test_download_subdirectory_skips_cleaned_up_file(tmp_path):
    remote_files = [
        {
            "name": "eeg.zip",
            "file": {"hashes": {"quickXorHash": REMOTE_HASH}},
            "@microsoft.graph.downloadUrl": "https://example.invalid/eeg.zip",
        }
    ]

    with patch(
        f"{UTILS}.DataPull.get_most_recent_data_pull_for_file_path",
        return_value=make_data_pull(tmp_path / "eeg.zip", REMOTE_HASH),
    ), patch(
        f"{UTILS}.File.version_has_any_pushes", return_value=True
    ), patch(
        f"{UTILS}.File.version_has_pending_pushes", return_value=False
    ), patch(
        f"{UTILS}.download_file"
    ) as download, patch(
        f"{UTILS}.db.execute_queries"
    ) as execute_queries:
        sharepoint_utils.download_subdirectory(
            remote_files,
            subject_id="BI11009",
            site_id=SITE_ID,
            project_id=PROJECT_ID,
            data_source_name="ProcanBI_sharepoint_eeg",
            output_dir=tmp_path,
            config_file=CONFIG_FILE,
            manage_deletions=False,
        )

    download.assert_not_called()
    execute_queries.assert_not_called()


def test_get_most_recent_data_pull_for_file_path_escapes_quotes():
    with patch("lochness.models.data_pulls.db.execute_sql") as execute_sql:
        execute_sql.return_value.empty = True
        result = DataPull.get_most_recent_data_pull_for_file_path(
            config_file=CONFIG_FILE, file_path="/data/O'Brien eeg.zip"
        )

    assert result is None
    query = execute_sql.call_args.args[1]
    assert "file_path = '/data/O''Brien eeg.zip'" in query
