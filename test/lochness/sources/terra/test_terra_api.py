"""
Unit tests for Terra REST API and Google Cloud Storage client helpers.
"""

import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

file = Path(__file__).resolve()
root_dir = file.parents[4]
if str(root_dir) not in sys.path:
    sys.path.insert(0, str(root_dir))

from lochness.sources.terra import api as terra_api

SAMPLE_SA_INFO = {
    "type": "service_account",
    "project_id": "test-gcp-project",
    "private_key_id": "key-123",
    "private_key": "-----BEGIN PRIVATE KEY-----\nFAKE\n-----END PRIVATE KEY-----\n",
    "client_email": "lochness-terra@test-gcp-project.iam.gserviceaccount.com",
    "client_id": "1234567890",
    "token_uri": "https://oauth2.googleapis.com/token",
}


class TestAuthentication:
    def test_get_credentials_from_service_account_info(self):
        fake_creds = MagicMock()
        with patch(
            "lochness.sources.terra.api.service_account.Credentials.from_service_account_info",
            return_value=fake_creds,
        ) as loader:
            creds = terra_api.get_credentials(SAMPLE_SA_INFO)

        assert creds is fake_creds
        loader.assert_called_once_with(
            SAMPLE_SA_INFO,
            scopes=list(terra_api.TERRA_OAUTH_SCOPES),
        )

    def test_get_credentials_from_nested_service_account_info(self):
        fake_creds = MagicMock()
        with patch(
            "lochness.sources.terra.api.service_account.Credentials.from_service_account_info",
            return_value=fake_creds,
        ) as loader:
            creds = terra_api.get_credentials({"service_account_info": SAMPLE_SA_INFO})

        assert creds is fake_creds
        loader.assert_called_once_with(
            SAMPLE_SA_INFO,
            scopes=list(terra_api.TERRA_OAUTH_SCOPES),
        )

    def test_get_credentials_from_service_account_file(self, tmp_path):
        sa_file = tmp_path / "sa.json"
        sa_file.write_text("{}", encoding="utf-8")
        fake_creds = MagicMock()
        with patch(
            "lochness.sources.terra.api.service_account.Credentials.from_service_account_file",
            return_value=fake_creds,
        ) as loader:
            creds = terra_api.get_credentials({"service_account_file": str(sa_file)})

        assert creds is fake_creds
        loader.assert_called_once_with(
            str(sa_file),
            scopes=list(terra_api.TERRA_OAUTH_SCOPES),
        )

    def test_get_credentials_missing_sa_file_raises(self, tmp_path):
        with pytest.raises(RuntimeError, match="Service account file not found"):
            terra_api.get_credentials(
                {"service_account_file": str(tmp_path / "missing.json")}
            )

    def test_get_credentials_missing_required_fields_raises(self):
        with pytest.raises(RuntimeError, match="missing fields"):
            terra_api.get_credentials({"client_email": "only@email.iam"})

    def test_get_credentials_adc_fallback_when_none(self):
        fake_creds = MagicMock()
        with patch(
            "lochness.sources.terra.api.google.auth.default",
            return_value=(fake_creds, "test-project"),
        ) as adc:
            creds = terra_api.get_credentials(None)

        assert creds is fake_creds
        adc.assert_called_once_with(scopes=list(terra_api.TERRA_OAUTH_SCOPES))

    def test_get_credentials_adc_explicit_mode(self):
        fake_creds = MagicMock()
        with patch(
            "lochness.sources.terra.api.google.auth.default",
            return_value=(fake_creds, "test-project"),
        ) as adc:
            creds = terra_api.get_credentials({"auth_mode": "adc"})

        assert creds is fake_creds
        adc.assert_called_once()

    def test_get_auth_headers_refreshes_expired_token(self):
        creds = MagicMock()
        creds.valid = False
        creds.token = None

        def do_refresh(_request):
            creds.valid = True
            creds.token = "fresh-bearer-token"

        creds.refresh.side_effect = do_refresh

        headers = terra_api.get_auth_headers(creds)
        assert headers == {"Authorization": "Bearer fresh-bearer-token"}
        creds.refresh.assert_called_once()

    def test_get_auth_headers_reuses_valid_token(self):
        creds = MagicMock()
        creds.valid = True
        creds.token = "existing-token"

        headers = terra_api.get_auth_headers(creds)
        assert headers == {"Authorization": "Bearer existing-token"}
        creds.refresh.assert_not_called()


class TestTerraRestApi:
    def test_get_workspace_success_and_url_encoding(self):
        mock_resp = MagicMock(status_code=200)
        mock_resp.json.return_value = {
            "workspace": {
                "namespace": "my-ns",
                "name": "my workspace",
                "bucketName": "fc-abc-123",
                "googleProject": "terra-proj-1",
            }
        }

        with patch("lochness.sources.terra.api.requests.get", return_value=mock_resp) as get:
            info = terra_api.get_workspace(
                workspace_namespace="my-ns",
                workspace_name="my workspace",
                headers={"Authorization": "Bearer tok"},
                api_url="https://api.firecloud.org/",
            )

        assert terra_api.get_workspace_bucket_name(info) == "fc-abc-123"
        get.assert_called_once_with(
            "https://api.firecloud.org/api/workspaces/my-ns/my%20workspace",
            headers={"Authorization": "Bearer tok"},
            timeout=120,
        )

    def test_get_workspace_error_raises(self):
        mock_resp = MagicMock(status_code=403, text="Forbidden")
        with (
            patch("lochness.sources.terra.api.requests.get", return_value=mock_resp),
            pytest.raises(RuntimeError, match="HTTP 403"),
        ):
            terra_api.get_workspace("ns", "ws", headers={})

    def test_get_workspace_bucket_name_missing_raises(self):
        with pytest.raises(RuntimeError, match="bucketName"):
            terra_api.get_workspace_bucket_name({"workspace": {}})

    def test_get_entity_types_success(self):
        mock_resp = MagicMock(status_code=200)
        mock_resp.json.return_value = {
            "participant": {"count": 10, "attributeNames": ["age"], "idName": "participant_id"},
            "sample": {"count": 20, "attributeNames": ["bam"], "idName": "sample_id"},
        }
        with patch("lochness.sources.terra.api.requests.get", return_value=mock_resp) as get:
            etypes = terra_api.get_entity_types("ns", "ws", headers={"Authorization": "Bearer x"})

        assert set(etypes.keys()) == {"participant", "sample"}
        get.assert_called_once_with(
            "https://api.firecloud.org/api/workspaces/ns/ws/entities",
            headers={"Authorization": "Bearer x"},
            timeout=120,
        )

    def test_get_entities_success(self):
        mock_resp = MagicMock(status_code=200)
        mock_resp.json.return_value = [
            {"name": "S1", "entityType": "sample", "attributes": {"bam": "gs://b/s1.bam"}}
        ]
        with patch("lochness.sources.terra.api.requests.get", return_value=mock_resp) as get:
            rows = terra_api.get_entities("ns", "ws", "sample", headers={"Authorization": "Bearer x"})

        assert len(rows) == 1
        assert rows[0]["name"] == "S1"
        get.assert_called_once_with(
            "https://api.firecloud.org/api/workspaces/ns/ws/entities/sample",
            headers={"Authorization": "Bearer x"},
            timeout=120,
        )


class TestGcsHelpers:
    def test_parse_gcs_uri_valid(self):
        assert terra_api.parse_gcs_uri("gs://fc-123/dir/sub/file.bam") == (
            "fc-123",
            "dir/sub/file.bam",
        )

    @pytest.mark.parametrize(
        "bad_uri",
        [
            "https://example.com/file.bam",
            "gs://only-bucket",
            "gs:///leading-slash.bam",
            "gs://fc-123/",
            "gs://fc-123/folder/",
            "",
        ],
    )
    def test_parse_gcs_uri_invalid_raises(self, bad_uri):
        with pytest.raises(ValueError):
            terra_api.parse_gcs_uri(bad_uri)

    def test_list_gcs_blobs_excludes_directory_placeholders(self):
        client = MagicMock()
        bucket = MagicMock()
        client.bucket.return_value = bucket
        client.list_blobs.return_value = [
            SimpleNamespace(name="raw/SUBJ1/"),
            SimpleNamespace(name="raw/SUBJ1/file1.csv"),
            SimpleNamespace(name="raw/SUBJ1/nested/file2.bam"),
        ]

        blobs = terra_api.list_gcs_blobs(
            client, "fc-bucket", prefix="raw/SUBJ1/", user_project="billing-proj"
        )

        client.bucket.assert_called_once_with("fc-bucket", user_project="billing-proj")
        client.list_blobs.assert_called_once_with(bucket, prefix="raw/SUBJ1/")
        assert [b.name for b in blobs] == [
            "raw/SUBJ1/file1.csv",
            "raw/SUBJ1/nested/file2.bam",
        ]

    def test_download_gcs_blob_atomic_success(self, tmp_path):
        target = tmp_path / "nested" / "sample.bam"
        blob = MagicMock()
        blob.name = "data/sample.bam"
        blob.bucket = SimpleNamespace(name="fc-bucket")

        def write_temp(path_str, timeout=300):
            assert path_str.endswith(".part")
            Path(path_str).write_bytes(b"BAMDATA")

        blob.download_to_filename.side_effect = write_temp

        terra_api.download_gcs_blob(blob, target)

        assert target.read_bytes() == b"BAMDATA"
        assert not any(p.name.endswith(".part") for p in target.parent.iterdir())

    def test_download_gcs_blob_cleans_up_temp_on_failure(self, tmp_path):
        target = tmp_path / "sample.bam"
        blob = MagicMock()
        blob.name = "data/sample.bam"
        blob.bucket = SimpleNamespace(name="fc-bucket")

        def fail_midway(path_str, timeout=300):
            Path(path_str).write_bytes(b"partial")
            raise OSError("network dropped")

        blob.download_to_filename.side_effect = fail_midway

        with pytest.raises(RuntimeError, match="Failed to download"):
            terra_api.download_gcs_blob(blob, target)

        assert not target.exists()
        assert not any(p.name.endswith(".part") for p in tmp_path.iterdir())
