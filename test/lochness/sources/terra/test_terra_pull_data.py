"""
Unit tests for TerraDataSource model, Terra data pull task, SupportedDataSourceTypes,
and Airflow DAG registration.
"""

import ast
import json
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pandas as pd

file = Path(__file__).resolve()
root_dir = file.parents[4]
if str(root_dir) not in sys.path:
    sys.path.insert(0, str(root_dir))

from lochness.models.keystore import KeyStore
from lochness.models.subjects import Subject
from lochness.models.supported_data_source_types import SUPPORTED_DATA_SOURCE_TYPES
from lochness.sources.terra.models.data_source import (
    TerraDataSource,
    TerraDataSourceMetadata,
)
from lochness.sources.terra.tasks import pull_data as terra_pull

CONFIG_FILE = Path("/nonexistent/config.ini")
PULL_MOD = "lochness.sources.terra.tasks.pull_data"
UTILS_MOD = "lochness.sources.terra.utils"


def make_terra_data_source(**metadata_overrides) -> TerraDataSource:
    defaults = {
        "keystore_name": "terra_prod",
        "workspace_namespace": "ampscz-billing",
        "workspace_name": "ampscz-genomics",
        "modality": "genomics",
        "pull_tables": True,
        "pull_table_gcs_files": True,
        "pull_bucket_files": True,
        "subject_id_column": "participant",
        "bucket_prefix": "raw_uploads",
    }
    defaults.update(metadata_overrides)
    return TerraDataSource(
        data_source_name="ProCAN_YA_terra",
        is_active=True,
        site_id="YA",
        project_id="ProCAN",
        data_source_type="terra",
        data_source_metadata=TerraDataSourceMetadata(**defaults),
    )


def make_subject(subject_id: str = "YA00001", metadata: dict | None = None) -> Subject:
    return Subject(
        subject_id=subject_id,
        site_id="YA",
        project_id="ProCAN",
        subject_metadata=metadata or {},
    )


def make_fake_blob(bucket_name: str, blob_name: str, payload: bytes, md5_hash: str):
    blob = MagicMock()
    blob.name = blob_name
    blob.md5_hash = md5_hash
    blob.crc32c = None
    blob.generation = 1
    blob.etag = "etag-1"
    blob.bucket = SimpleNamespace(name=bucket_name)

    def download_to_filename(path_str: str, timeout: int = 300):
        Path(path_str).write_bytes(payload)

    blob.download_to_filename.side_effect = download_to_filename
    return blob


class TestSupportedDataSourceAndModel:
    def test_terra_registered_in_supported_data_source_types(self):
        types = {t.data_source_type: t for t in SUPPORTED_DATA_SOURCE_TYPES}
        assert "terra" in types
        meta = types["terra"].data_source_metadata_dict
        assert "workspace_namespace" in meta
        assert "workspace_name" in meta
        assert "keystore_name" in meta

    def test_get_all_terra_data_sources_parses_rows(self):
        df = pd.DataFrame(
            [
                {
                    "data_source_name": "ProCAN_YA_terra",
                    "data_source_is_active": True,
                    "site_id": "YA",
                    "project_id": "ProCAN",
                    "data_source_type": "terra",
                    "data_source_metadata": {
                        "keystore_name": "terra_prod",
                        "workspace_namespace": "my-ns",
                        "workspace_name": "my-ws",
                        "api_url": "https://api.firecloud.org/",
                    },
                }
            ]
        )

        with patch("lochness.sources.terra.models.data_source.db.execute_sql", return_value=df):
            sources = TerraDataSource.get_all_terra_data_sources(CONFIG_FILE)

        assert len(sources) == 1
        ds = sources[0]
        assert ds.data_source_type == "terra"
        assert ds.data_source_metadata.workspace_namespace == "my-ns"
        assert ds.data_source_metadata.workspace_name == "my-ws"
        assert ds.data_source_metadata.api_url == "https://api.firecloud.org"
        assert ds.data_source_metadata.modality == "terra"

    def test_terra_dag_file_loads_cleanly(self):
        import importlib.util

        dag_path = root_dir / "dags" / "terra_pull_data.py"
        assert dag_path.is_file()
        source = dag_path.read_text(encoding="utf-8")
        ast.parse(source)

        spec = importlib.util.spec_from_file_location("terra_pull_data_dag", dag_path)
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        assert module.dag.dag_id == "lochness_terra_pull_data"
        task_ids = set(module.dag.task_dict.keys())
        assert task_ids == {"print_info", "terra_pull_data", "trigger_push_data"}
        assert {"project_id", "site_id", "source_id", "dry_run", "force_redownload"}.issubset(
            set(module.dag.params.keys())
        )


class TestFetchWorkspaceData:
    def test_pulls_tables_table_gcs_files_and_bucket_files(self, tmp_path):
        ds = make_terra_data_source(subject_id_metadata_key="terra_id")
        subject = make_subject("YA00001", {"terra_id": "T_YA00001"})

        keystore = KeyStore(
            key_name="terra_prod",
            key_value=json.dumps({"auth_mode": "adc"}),
            key_type="terra",
            project_id="ProCAN",
        )
        fake_creds = MagicMock()
        fake_storage_client = MagicMock()

        table_blob = make_fake_blob(
            "fc-ws-bucket",
            "aligned/T_YA00001.vcf.gz",
            b"VCF_CONTENT",
            md5_hash="vcf-md5==",
        )
        bucket_blob = make_fake_blob(
            "fc-ws-bucket",
            "raw_uploads/T_YA00001/qc/report.txt",
            b"QC_REPORT",
            md5_hash="qc-md5==",
        )

        sample_entities = [
            {
                "name": "SAMPLE_1",
                "entityType": "sample",
                "attributes": {
                    "participant": {
                        "entityType": "participant",
                        "entityName": "T_YA00001",
                    },
                    "vcf_file": "gs://fc-ws-bucket/aligned/T_YA00001.vcf.gz",
                },
            }
        ]

        with patch(
            f"{PULL_MOD}.config.parse",
            return_value={"lochness_root": str(tmp_path)},
        ), patch(
            f"{UTILS_MOD}.config.parse",
            return_value={"lochness_root": str(tmp_path)},
        ), patch(
            f"{PULL_MOD}.KeyStore.retrieve_keystore",
            return_value=keystore,
        ), patch(
            f"{PULL_MOD}.terra_api.get_credentials",
            return_value=fake_creds,
        ), patch(
            f"{PULL_MOD}.terra_api.get_auth_headers",
            return_value={"Authorization": "Bearer fake"},
        ), patch(
            f"{PULL_MOD}.terra_api.get_workspace",
            return_value={
                "workspace": {
                    "bucketName": "fc-ws-bucket",
                    "googleProject": "terra-gcp-proj",
                }
            },
        ), patch(
            f"{PULL_MOD}.terra_api.get_gcs_credentials",
            return_value=fake_creds,
        ), patch(
            f"{PULL_MOD}.terra_api.create_storage_client",
            return_value=fake_storage_client,
        ) as create_storage_client, patch(
            f"{PULL_MOD}.terra_api.get_entity_types",
            return_value={"sample": {"count": 1}},
        ), patch(
            f"{PULL_MOD}.terra_api.get_entities",
            return_value=sample_entities,
        ), patch(
            f"{PULL_MOD}.terra_api.get_gcs_blob_metadata",
            return_value=table_blob,
        ), patch(
            f"{PULL_MOD}.terra_api.list_gcs_blobs",
            return_value=[bucket_blob],
        ), patch(
            f"{UTILS_MOD}.DataPull.get_most_recent_data_pull_for_file_path",
            return_value=None,
        ), patch(
            f"{UTILS_MOD}.db.execute_queries"
        ) as execute_queries, patch(
            f"{UTILS_MOD}.log_event"
        ):
            stats = terra_pull.fetch_workspace_data_for_subjects(
                terra_data_source=ds,
                subjects=[subject],
                config_file=CONFIG_FILE,
            )

        assert stats == {
            "tables_saved": 1,
            "files_downloaded": 2,
            "files_skipped": 0,
            "errors": 0,
        }

        phoenix_modality_dir = (
            tmp_path
            / "Procan"
            / "PHOENIX"
            / "PROTECTED"
            / "ProcanYA"
            / "raw"
            / "YA00001"
            / "genomics"
        )
        table_path = phoenix_modality_dir / "tables" / "sample_YA00001.json"
        table_gcs_path = (
            phoenix_modality_dir
            / "table_files"
            / "sample"
            / "fc-ws-bucket"
            / "aligned"
            / "T_YA00001.vcf.gz"
        )
        bucket_file_path = phoenix_modality_dir / "bucket_files" / "qc" / "report.txt"

        assert table_path.is_file()
        assert json.loads(table_path.read_text())[0]["name"] == "SAMPLE_1"
        assert table_gcs_path.read_bytes() == b"VCF_CONTENT"
        assert bucket_file_path.read_bytes() == b"QC_REPORT"
        assert execute_queries.call_count == 3
        create_storage_client.assert_called_once_with(
            credentials=fake_creds,
            project=None,
        )

        # Re-running with identical remote versions skips all 3 items
        with patch(
            f"{PULL_MOD}.config.parse",
            return_value={"lochness_root": str(tmp_path)},
        ), patch(
            f"{PULL_MOD}.KeyStore.retrieve_keystore",
            return_value=keystore,
        ), patch(
            f"{PULL_MOD}.terra_api.get_credentials",
            return_value=fake_creds,
        ), patch(
            f"{PULL_MOD}.terra_api.get_gcs_credentials",
            return_value=fake_creds,
        ), patch(
            f"{PULL_MOD}.terra_api.get_auth_headers",
            return_value={"Authorization": "Bearer fake"},
        ), patch(
            f"{PULL_MOD}.terra_api.get_workspace",
            return_value={
                "workspace": {
                    "bucketName": "fc-ws-bucket",
                    "googleProject": "terra-gcp-proj",
                }
            },
        ), patch(
            f"{PULL_MOD}.terra_api.create_storage_client",
            return_value=fake_storage_client,
        ), patch(
            f"{PULL_MOD}.terra_api.get_entity_types",
            return_value={"sample": {"count": 1}},
        ), patch(
            f"{PULL_MOD}.terra_api.get_entities",
            return_value=sample_entities,
        ), patch(
            f"{PULL_MOD}.terra_api.get_gcs_blob_metadata",
            return_value=table_blob,
        ), patch(
            f"{PULL_MOD}.terra_api.list_gcs_blobs",
            return_value=[bucket_blob],
        ), patch(
            f"{UTILS_MOD}.db.execute_queries"
        ) as execute_queries_second:
            second_stats = terra_pull.fetch_workspace_data_for_subjects(
                terra_data_source=ds,
                subjects=[subject],
                config_file=CONFIG_FILE,
            )

        assert second_stats == {
            "tables_saved": 0,
            "files_downloaded": 0,
            "files_skipped": 3,
            "errors": 0,
        }
        execute_queries_second.assert_not_called()

    def test_dry_run_does_not_write_files_or_db(self, tmp_path):
        ds = make_terra_data_source(
            keystore_name=None,
            pull_tables=True,
            pull_table_gcs_files=False,
            pull_bucket_files=False,
            subject_id_column="name",
        )
        subject = make_subject("YA00001")

        with patch(
            f"{PULL_MOD}.config.parse",
            return_value={"lochness_root": str(tmp_path)},
        ), patch(
            f"{PULL_MOD}.terra_api.get_credentials",
            return_value=MagicMock(),
        ), patch(
            f"{PULL_MOD}.terra_api.get_auth_headers",
            return_value={"Authorization": "Bearer fake"},
        ), patch(
            f"{PULL_MOD}.terra_api.get_entity_types",
            return_value={"participant": {"count": 1}},
        ), patch(
            f"{PULL_MOD}.terra_api.get_entities",
            return_value=[{"name": "YA00001", "entityType": "participant", "attributes": {}}],
        ), patch(
            f"{UTILS_MOD}.DataPull.get_most_recent_data_pull_for_file_path",
            return_value=None,
        ), patch(
            f"{UTILS_MOD}.db.execute_queries"
        ) as execute_queries:
            stats = terra_pull.fetch_workspace_data_for_subjects(
                terra_data_source=ds,
                subjects=[subject],
                config_file=CONFIG_FILE,
                dry_run=True,
            )

        assert stats["tables_saved"] == 1
        assert list(tmp_path.rglob("*")) == []
        execute_queries.assert_not_called()

    def test_load_terra_credentials_persists_rotated_b2c_refresh_token(self):
        ds = make_terra_data_source()
        keystore = KeyStore(
            key_name="terra_prod",
            key_value=json.dumps(
                {"auth_mode": "terra_b2c", "refresh_token": "old-rt"}
            ),
            key_type="terra",
            project_id="ProCAN",
        )

        with (
            patch(f"{PULL_MOD}.KeyStore.retrieve_keystore", return_value=keystore),
            patch(
                f"{PULL_MOD}.config.get_encryption_passphrase",
                return_value="secret-passphrase",
            ),
            patch(f"{PULL_MOD}.db.execute_queries") as exec_queries,
        ):
            creds = terra_pull._load_terra_credentials(ds, CONFIG_FILE)
            assert isinstance(creds, terra_pull.terra_api.TerraB2CCredentials)
            assert creds.on_refresh_token_rotated is not None
            creds.on_refresh_token_rotated("new-rotated-rt")

        exec_queries.assert_called_once()
        sql_list = exec_queries.call_args.kwargs["queries"]
        assert len(sql_list) == 1
        assert "new-rotated-rt" in sql_list[0]

    def test_pull_workspace_bucket_mirrors_all_blobs(self, tmp_path):
        ds = make_terra_data_source(
            keystore_name=None,
            pull_tables=False,
            pull_table_gcs_files=False,
            pull_bucket_files=False,
            pull_workspace_bucket=True,
            bucket_name="fc-cohort-bucket",
            bucket_prefix="",
            max_workers=4,
        )
        cohort_subject = Subject(
            subject_id="WORKSPACE_COHORT",
            site_id="COHORT",
            project_id="ProNET",
            subject_metadata={},
        )
        ds.site_id = "COHORT"
        ds.project_id = "ProNET"

        b1 = make_fake_blob(
            "fc-cohort-bucket",
            "submissions/sub1/wf/call-1/stdout",
            b"LOG1",
            md5_hash="m1==",
        )
        b2 = make_fake_blob(
            "fc-cohort-bucket",
            "submissions/sub1/wf/call-2/output.gvcf.gz",
            b"GVCFDATA",
            md5_hash="m2==",
        )

        with (
            patch(f"{PULL_MOD}.config.parse", return_value={"lochness_root": str(tmp_path)}),
            patch(f"{PULL_MOD}.terra_api.get_credentials", return_value=MagicMock()),
            patch(
                f"{PULL_MOD}.terra_api.get_auth_headers",
                return_value={"Authorization": "Bearer tok"},
            ),
            patch(f"{PULL_MOD}.terra_api.get_gcs_credentials", return_value=MagicMock()),
            patch(f"{PULL_MOD}.terra_api.create_storage_client", return_value=MagicMock()),
            patch(f"{PULL_MOD}.terra_api.iter_gcs_blobs", return_value=iter([b1, b2])),
            patch(
                f"{UTILS_MOD}.DataPull.get_most_recent_data_pull_for_file_path",
                return_value=None,
            ),
            patch(f"{UTILS_MOD}.db.execute_queries") as exec_q,
            patch(f"{UTILS_MOD}.log_event"),
        ):
            stats = terra_pull.fetch_workspace_data_for_subjects(
                terra_data_source=ds,
                subjects=[cohort_subject],
                config_file=CONFIG_FILE,
            )

        assert stats == {
            "tables_saved": 0,
            "files_downloaded": 2,
            "files_skipped": 0,
            "errors": 0,
        }
        exec_q.assert_called_once()
        out_base = (
            tmp_path
            / "Pronet"
            / "PHOENIX"
            / "PROTECTED"
            / "PronetCOHORT"
            / "raw"
            / "WORKSPACE_COHORT"
            / "genomics"
            / "bucket_files"
        )
        assert (out_base / "submissions/sub1/wf/call-1/stdout").read_bytes() == b"LOG1"
        assert (
            out_base / "submissions/sub1/wf/call-2/output.gvcf.gz"
        ).read_bytes() == b"GVCFDATA"


class TestPullAllData:
    def test_pull_all_data_filters_sources_and_subjects(self):
        ds1 = make_terra_data_source()
        ds2 = TerraDataSource(
            data_source_name="Other_terra",
            is_active=True,
            site_id="OR",
            project_id="OtherProj",
            data_source_type="terra",
            data_source_metadata=TerraDataSourceMetadata(
                workspace_namespace="ns2", workspace_name="ws2"
            ),
        )
        subjects = [make_subject("YA00001"), make_subject("YA00002")]

        with patch(
            f"{PULL_MOD}.TerraDataSource.get_all_terra_data_sources",
            return_value=[ds1, ds2],
        ), patch(
            f"{PULL_MOD}.Subject.get_subjects_for_project_site",
            return_value=subjects,
        ), patch(
            f"{PULL_MOD}.fetch_workspace_data_for_subjects",
            return_value={
                "tables_saved": 1,
                "files_downloaded": 2,
                "files_skipped": 0,
                "errors": 0,
            },
        ) as fetch_fn, patch(
            f"{PULL_MOD}.terra_utils.log_event"
        ):
            stats = terra_pull.pull_all_data(
                config_file=CONFIG_FILE,
                project_id="ProCAN",
                site_id="YA",
                subject_id_list=["YA00001"],
                source_id="ProCAN_YA_terra",
            )

        assert stats == {
            "tables_saved": 1,
            "files_downloaded": 2,
            "files_skipped": 0,
            "errors": 0,
        }
        fetch_fn.assert_called_once()
        passed_subjects = fetch_fn.call_args.kwargs["subjects"]
        assert [s.subject_id for s in passed_subjects] == ["YA00001"]

    def test_pull_all_data_no_active_sources(self):
        with patch(
            f"{PULL_MOD}.TerraDataSource.get_all_terra_data_sources",
            return_value=[],
        ), patch(f"{PULL_MOD}.terra_utils.log_event") as log_ev:
            stats = terra_pull.pull_all_data(config_file=CONFIG_FILE)

        assert stats == {
            "tables_saved": 0,
            "files_downloaded": 0,
            "files_skipped": 0,
            "errors": 0,
        }
        events = [call.kwargs["event"] for call in log_ev.call_args_list]
        assert "terra_data_pull_no_active_sources" in events
