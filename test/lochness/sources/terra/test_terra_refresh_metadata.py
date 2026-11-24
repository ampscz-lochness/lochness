"""
Unit tests for Terra workspace, site, and subject auto-discovery (refresh_metadata).
"""

import sys
from pathlib import Path

file = Path(__file__).resolve()
root_dir = file.parents[4]
if str(root_dir) not in sys.path:
    sys.path.insert(0, str(root_dir))

from lochness.sources.terra.tasks import refresh_metadata as terra_refresh


class TestTerraRefreshMetadata:
    def test_infer_project_id(self):
        assert (
            terra_refresh.infer_project_id(
                "martin-amp-scz-dpacc", "AMP-SCZ_Martin_Nelson_PRESCIENT_BGE"
            )
            == "PRESCIENT"
        )
        assert (
            terra_refresh.infer_project_id(
                "martin-amp-scz-dpacc", "AMP-SCZ_Martin_Woods_ProNET_BGE"
            )
            == "ProNET"
        )

    def test_filter_target_workspaces_excludes_public(self):
        raw = [
            {
                "public": True,
                "workspace": {"namespace": "featured-ampscz", "name": "Demo"},
            },
            {
                "public": False,
                "accessLevel": "READER",
                "workspace": {
                    "namespace": "martin-amp-scz-dpacc",
                    "name": "AMP-SCZ_Martin_Woods_ProNET_BGE",
                    "bucketName": "fc-84618ddf",
                },
            },
            {
                "public": False,
                "accessLevel": "READER",
                "workspace": {
                    "namespace": "unrelated-lab",
                    "name": "Other_Study",
                },
            },
        ]
        filtered = terra_refresh.filter_target_workspaces(
            raw, namespace_patterns=["*amp*scz*"]
        )
        assert len(filtered) == 1
        assert filtered[0][0] == "martin-amp-scz-dpacc"
        assert filtered[0][1] == "AMP-SCZ_Martin_Woods_ProNET_BGE"

    def test_build_workspace_onboarding_queries_creates_sites_subjects_and_cohort(self):
        tables = {
            "sample": [
                {
                    "name": "SM-1",
                    "entityType": "sample",
                    "attributes": {"collaborator_participant_id": "1036-NC28488"},
                },
                {
                    "name": "SM-2",
                    "entityType": "sample",
                    "attributes": {"collaborator_participant_id": "1036-YA00123"},
                },
            ],
            "sample_set": [
                {
                    "name": "batch_all",
                    "entityType": "sample_set",
                    "attributes": {},
                }
            ],
        }
        ws_info = {"workspace": {"bucketName": "fc-84618ddf"}}
        cred_dict = {"auth_mode": "terra_b2c", "refresh_token": "rt-xyz"}

        queries, summary = terra_refresh.build_workspace_onboarding_queries(
            workspace_namespace="martin-amp-scz-dpacc",
            workspace_name="AMP-SCZ_Martin_Woods_ProNET_BGE",
            workspace_info=ws_info,
            tables=tables,
            cred_dict=cred_dict,
            encryption_passphrase="secret",
        )

        assert summary["project_id"] == "ProNET"
        assert summary["sites_discovered"] == ["NC", "YA"]
        assert summary["subject_count"] == 2
        assert summary["cohort_unmatched_entity_rows"] == 1

        joined_sql = "\n".join(queries)
        assert "'ProNET'" in joined_sql
        assert "'NC28488'" in joined_sql
        assert "'YA00123'" in joined_sql
        assert "'WORKSPACE_COHORT'" in joined_sql
        assert "'ProNET_NC_terra'" in joined_sql
        assert "'ProNET_COHORT_terra'" in joined_sql
        assert '"pull_workspace_bucket": true' in joined_sql
