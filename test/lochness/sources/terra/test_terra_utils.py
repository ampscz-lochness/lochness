"""
Unit tests for Terra utility functions, entity/GCS parsing, and download skip logic.
"""

import csv
import io
import json
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

file = Path(__file__).resolve()
root_dir = file.parents[4]
if str(root_dir) not in sys.path:
    sys.path.insert(0, str(root_dir))

from lochness.models.data_pulls import DataPull
from lochness.models.subjects import Subject
from lochness.sources.terra import utils as terra_utils

CONFIG_FILE = Path("/nonexistent/config.ini")
PROJECT_ID = "ProCAN"
SITE_ID = "YA"
REMOTE_VERSION = "md5:abc123=="
UTILS = "lochness.sources.terra.utils"


def make_data_pull(file_path: Path, remote_version: str) -> DataPull:
    return DataPull(
        subject_id="YA00001",
        data_source_name="ProCAN_YA_terra",
        site_id=SITE_ID,
        project_id=PROJECT_ID,
        file_path=str(file_path),
        file_md5="deadbeef",
        pull_time_s=3,
        pull_metadata={"remote_version": remote_version},
    )


class TestPathAndSubjectHelpers:
    def test_build_subject_output_dir(self):
        out = terra_utils.build_subject_output_dir(
            lochness_root="/var/lib/PHOENIX",
            project_id="ProCAN",
            site_id="YA",
            subject_id="YA00001",
            modality="genomics",
        )
        assert out == Path(
            "/var/lib/PHOENIX/Procan/PHOENIX/PROTECTED/ProcanYA/raw/YA00001/genomics"
        )

    def test_resolve_terra_subject_id_defaults_to_subject_id(self):
        subj = Subject(
            subject_id="YA00001",
            site_id="YA",
            project_id="ProCAN",
            subject_metadata={},
        )
        assert terra_utils.resolve_terra_subject_id(subj) == "YA00001"

    def test_resolve_terra_subject_id_top_level_and_nested_metadata(self):
        subj = Subject(
            subject_id="YA00001",
            site_id="YA",
            project_id="ProCAN",
            subject_metadata={
                "terra_id": "TERRA_001",
                "terra": {"participant_id": "PART_999"},
            },
        )
        assert terra_utils.resolve_terra_subject_id(subj, "terra_id") == "TERRA_001"
        assert (
            terra_utils.resolve_terra_subject_id(subj, "terra.participant_id")
            == "PART_999"
        )
        assert terra_utils.resolve_terra_subject_id(subj, "missing.key") is None

    def test_safe_relative_blob_path(self):
        assert terra_utils.safe_relative_blob_path(
            "data/SUBJ1/sub/file.vcf.gz", strip_prefix="data/SUBJ1/"
        ) == Path("sub/file.vcf.gz")

    @pytest.mark.parametrize(
        "unsafe_blob",
        ["", "/", "../secret.txt", "a/../../escape.txt"],
    )
    def test_safe_relative_blob_path_rejects_traversal(self, unsafe_blob):
        with pytest.raises(ValueError):
            terra_utils.safe_relative_blob_path(unsafe_blob)


class TestEntityParsingAndSerialization:
    def test_extract_entity_subject_id_variants(self):
        e_by_name = {"name": "YA00001", "entityType": "participant", "attributes": {}}
        assert terra_utils.extract_entity_subject_id(e_by_name, "name") == "YA00001"

        e_by_str = {
            "name": "SMP_1",
            "entityType": "sample",
            "attributes": {"subject_id": "YA00001"},
        }
        assert terra_utils.extract_entity_subject_id(e_by_str, "subject_id") == "YA00001"

        e_by_ref = {
            "name": "SMP_2",
            "entityType": "sample",
            "attributes": {
                "participant": {
                    "entityType": "participant",
                    "entityName": "YA00002",
                }
            },
        }
        assert (
            terra_utils.extract_entity_subject_id(e_by_ref, "participant") == "YA00002"
        )

    def test_group_entities_by_subject_sorts_deterministically(self):
        entities = [
            {"name": "S2", "entityType": "sample", "attributes": {"participant": "YA1"}},
            {"name": "S1", "entityType": "sample", "attributes": {"participant": "YA1"}},
            {"name": "S3", "entityType": "sample", "attributes": {"participant": "YA2"}},
        ]
        grouped = terra_utils.group_entities_by_subject(entities, "participant")
        assert [e["name"] for e in grouped["YA1"]] == ["S1", "S2"]
        assert [e["name"] for e in grouped["YA2"]] == ["S3"]

    def test_extract_gcs_uris_handles_strings_lists_and_terra_typed_arrays(self):
        entity = {
            "name": "S1",
            "entityType": "sample",
            "attributes": {
                "bam_path": "gs://fc-bucket/bams/s1.bam",
                "qc_files": {
                    "itemsType": "AttributeValue",
                    "items": [
                        "gs://fc-bucket/qc/s1_metrics.txt",
                        "gs://fc-bucket/bams/s1.bam",  # duplicate
                    ],
                },
                "extra_paths": ["gs://other-bucket/vcf/s1.vcf.gz", "not-a-uri"],
                "folder_only": "gs://fc-bucket/just-a-folder/",
                "age": 24,
            },
        }

        all_uris = terra_utils.extract_gcs_uris_from_entity(entity)
        assert all_uris == [
            ("bam_path", "gs://fc-bucket/bams/s1.bam"),
            ("extra_paths", "gs://other-bucket/vcf/s1.vcf.gz"),
            ("qc_files", "gs://fc-bucket/qc/s1_metrics.txt"),
        ]

        filtered = terra_utils.extract_gcs_uris_from_entity(
            entity, gcs_uri_columns=["bam_path"]
        )
        assert filtered == [("bam_path", "gs://fc-bucket/bams/s1.bam")]

    def test_matches_file_patterns(self):
        assert terra_utils.matches_file_patterns(
            "data/SUBJ1/sample.vcf.gz",
            include_patterns=["*.vcf.gz", "*.bam"],
            exclude_patterns=["*tmp*"],
        )
        assert not terra_utils.matches_file_patterns(
            "data/SUBJ1/tmp_sample.vcf.gz",
            include_patterns=["*.vcf.gz"],
            exclude_patterns=["*tmp*"],
        )
        assert not terra_utils.matches_file_patterns(
            "data/SUBJ1/readme.txt",
            include_patterns=["*.vcf.gz"],
        )

    def test_get_remote_blob_version_precedence(self):
        assert (
            terra_utils.get_remote_blob_version(
                SimpleNamespace(md5_hash="m1", crc32c="c1", generation=10, etag="e1")
            )
            == "md5:m1"
        )
        # Composite GCS objects lack md5_hash and fall back to crc32c
        assert (
            terra_utils.get_remote_blob_version(
                SimpleNamespace(md5_hash=None, crc32c="c1", generation=10, etag="e1")
            )
            == "crc32c:c1"
        )
        assert (
            terra_utils.get_remote_blob_version(
                SimpleNamespace(md5_hash=None, crc32c=None, generation=10, etag="e1")
            )
            == "gen:10"
        )
        assert (
            terra_utils.get_remote_blob_version(
                SimpleNamespace(md5_hash=None, crc32c=None, generation=None, etag="e1")
            )
            == "etag:e1"
        )

    def test_serialize_entities_deterministic_json_and_tsv(self):
        entities_a = [
            {"name": "S2", "entityType": "sample", "attributes": {"b": 2, "a": 1}},
            {"name": "S1", "entityType": "sample", "attributes": {"a": 10, "c": {"k": "v"}}},
        ]
        entities_b = list(reversed(entities_a))

        json_a = terra_utils.serialize_entities(entities_a, table_format="json")
        json_b = terra_utils.serialize_entities(entities_b, table_format="json")
        assert json_a == json_b
        parsed = json.loads(json_a.decode("utf-8"))
        assert [e["name"] for e in parsed] == ["S1", "S2"]

        tsv_bytes = terra_utils.serialize_entities(entities_a, table_format="tsv")
        tsv_rows = list(
            csv.DictReader(io.StringIO(tsv_bytes.decode("utf-8")), delimiter="\t")
        )
        assert list(tsv_rows[0].keys()) == ["name", "entityType", "a", "b", "c"]
        assert tsv_rows[0] == {
            "name": "S1",
            "entityType": "sample",
            "a": "10",
            "b": "",
            "c": '{"k": "v"}',
        }
        assert tsv_rows[1] == {
            "name": "S2",
            "entityType": "sample",
            "a": "1",
            "b": "2",
            "c": "",
        }

        csv_bytes = terra_utils.serialize_entities(entities_a, table_format="csv")
        csv_rows = list(csv.DictReader(io.StringIO(csv_bytes.decode("utf-8"))))
        assert csv_rows == tsv_rows


class TestShouldDownloadFile:
    def test_skips_when_local_file_and_sidecar_match(self, tmp_path):
        local_file = tmp_path / "sample.bam"
        local_file.write_bytes(b"data")
        terra_utils.get_version_sidecar_path(local_file).write_text(REMOTE_VERSION)

        with patch(f"{UTILS}.DataPull.get_most_recent_data_pull_for_file_path") as lookup:
            assert (
                terra_utils.should_download_file(
                    local_file,
                    REMOTE_VERSION,
                    config_file=CONFIG_FILE,
                    project_id=PROJECT_ID,
                    site_id=SITE_ID,
                )
                is False
            )
            lookup.assert_not_called()

    def test_force_redownload_overrides_local_match(self, tmp_path):
        local_file = tmp_path / "sample.bam"
        local_file.write_bytes(b"data")
        terra_utils.get_version_sidecar_path(local_file).write_text(REMOTE_VERSION)

        assert (
            terra_utils.should_download_file(
                local_file,
                REMOTE_VERSION,
                force_redownload=True,
            )
            is True
        )

    def test_downloads_when_local_version_differs(self, tmp_path):
        local_file = tmp_path / "sample.bam"
        local_file.write_bytes(b"data")
        terra_utils.get_version_sidecar_path(local_file).write_text("md5:old==")

        assert terra_utils.should_download_file(local_file, REMOTE_VERSION) is True

    def test_skips_when_cleaned_up_after_push(self, tmp_path):
        """Files removed by post-push cleanup must not be re-downloaded if unchanged."""
        local_file = tmp_path / "sample.bam"

        with patch(
            f"{UTILS}.DataPull.get_most_recent_data_pull_for_file_path",
            return_value=make_data_pull(local_file, REMOTE_VERSION),
        ), patch(
            f"{UTILS}.File.version_has_any_pushes", return_value=True
        ), patch(
            f"{UTILS}.File.version_has_pending_pushes", return_value=False
        ) as pending:
            assert (
                terra_utils.should_download_file(
                    local_file,
                    REMOTE_VERSION,
                    config_file=CONFIG_FILE,
                    project_id=PROJECT_ID,
                    site_id=SITE_ID,
                )
                is False
            )
            pending.assert_called_once_with(
                config_file=CONFIG_FILE,
                file_path=local_file,
                file_md5="deadbeef",
                project_id=PROJECT_ID,
                site_id=SITE_ID,
            )

    def test_downloads_when_remote_version_changed_after_cleanup(self, tmp_path):
        local_file = tmp_path / "sample.bam"

        with patch(
            f"{UTILS}.DataPull.get_most_recent_data_pull_for_file_path",
            return_value=make_data_pull(local_file, "md5:old=="),
        ):
            assert (
                terra_utils.should_download_file(
                    local_file,
                    REMOTE_VERSION,
                    config_file=CONFIG_FILE,
                    project_id=PROJECT_ID,
                    site_id=SITE_ID,
                )
                is True
            )

    def test_downloads_when_push_still_pending(self, tmp_path):
        local_file = tmp_path / "sample.bam"

        with patch(
            f"{UTILS}.DataPull.get_most_recent_data_pull_for_file_path",
            return_value=make_data_pull(local_file, REMOTE_VERSION),
        ), patch(
            f"{UTILS}.File.version_has_any_pushes", return_value=True
        ), patch(
            f"{UTILS}.File.version_has_pending_pushes", return_value=True
        ):
            assert (
                terra_utils.should_download_file(
                    local_file,
                    REMOTE_VERSION,
                    config_file=CONFIG_FILE,
                    project_id=PROJECT_ID,
                    site_id=SITE_ID,
                )
                is True
            )
