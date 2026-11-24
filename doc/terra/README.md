# Terra (app.terra.bio) Data Source Guide

Lochness supports pulling data from **Terra on Google Cloud Platform** (`app.terra.bio`), including:

1. **Workspace Data Tables (Entities)** — exported per subject as deterministic `json`, `tsv`, or `csv` files via the Terra/FireCloud REST API.
2. **Table-Referenced GCS Files** — `gs://...` objects referenced inside matched entity table columns.
3. **Workspace GCS Bucket Files** — files stored directly under subject folders in the workspace's `gs://fc-...` bucket.

## 1. Authentication Options

Lochness supports **both** Terra authentication methods:

### Option A: Terra Microsoft / Azure AD B2C Login (`TERRA_REFRESH_TOKEN` / `config.ini`)
If you sign into `app.terra.bio` using **Sign in with Microsoft** (institutional Entra ID / Azure AD B2C), authentication on a **headless (no-GUI) server** works just like SharePoint:

1. **Insert credentials into `KeyStore` on the headless server** (via `TERRA_REFRESH_TOKEN` or `config.ini` `[terra]`):
   ```bash
   export TERRA_REFRESH_TOKEN="<your_terra_b2c_refresh_token>"
   python test/lochness/sources/terra/insert_terra_credentials.py \
     --key-name terra_prod \
     --project-id ProCAN
   ```
   Or set `refresh_token` under `[terra]` in `config.ini`:
   ```ini
   [terra]
   refresh_token=your_terra_b2c_refresh_token
   ```
   *(To generate a fresh `TERRA_REFRESH_TOKEN` on your laptop at any time, run `python test/lochness/sources/terra/insert_terra_credentials.py --get-refresh-token`.)*

2. **100% non-interactive runtime in Airflow / Cron**:
   - During `pull_data.py`, Lochness reads the encrypted `refresh_token` from PostgreSQL `key_store`, exchanges it via HTTPS POST for a fresh Terra Bearer token, **automatically persists any rotated `refresh_token` back into PostgreSQL `key_store`**, and exchanges the Terra token via Terra Sam (`POST /api/google/v1/user/petServiceAccount/token`) for a temporary GCS token. **No browser or GUI is ever used on the server.**

### Option B: Google Cloud Service Account JSON Key
For non-personal automation pipelines:
1. Create a Google Cloud Service Account in your GCP project and download its JSON key file.
2. Register the Service Account email in Terra (`broadinstitute/firecloud-tools` `register_service_account.py`) and grant it **Reader** access on the target Terra workspace (`app.terra.bio` → Workspace → Share).
   - If pulling from Requester Pays GCS buckets, grant the Service Account `Service Usage Consumer` on your billing project and set `billing_project` in the data source metadata.
3. Insert the Service Account JSON key into Lochness `KeyStore`:

```bash
python test/lochness/sources/terra/insert_terra_credentials.py \
  --key-name terra_prod \
  --project-id ProCAN \
  --sa-json /path/to/terra-service-account.json
```

Alternatively, omit `keystore_name` (or store `{"auth_mode": "adc"}`) to use Google Application Default Credentials (`GOOGLE_APPLICATION_CREDENTIALS` or GCE/GKE workload identity).

Ensure `supported_data_source_types` includes `terra` (populated automatically by `lochness/scripts/init_db.py` or `test/lochness/sources/terra/insert_terra_data_source.py`).

## 2. Register a Terra Data Source

```bash
python test/lochness/sources/terra/insert_terra_data_source.py \
  --name ProCAN_YA_terra \
  --project-id ProCAN \
  --site-id YA \
  --workspace-namespace my-terra-billing-project \
  --workspace-name my-terra-workspace \
  --keystore-name terra_prod \
  --modality terra \
  --subject-id-column participant \
  --pull-bucket-files
```

### Supported `data_source_metadata` Fields

| Field | Default | Description |
|---|---|---|
| `workspace_namespace` | *(required)* | Terra billing project / workspace namespace |
| `workspace_name` | *(required)* | Terra workspace name |
| `keystore_name` | `null` | Lochness KeyStore entry containing GCP Service Account JSON (`null` uses ADC) |
| `modality` | `"terra"` | PHOENIX modality subdirectory under `raw/{subject_id}/{modality}` |
| `api_url` | `"https://api.firecloud.org"` | Terra/FireCloud API base URL |
| `pull_tables` | `true` | Save matched entity table rows per subject |
| `pull_table_gcs_files` | `true` | Download `gs://` URIs referenced in matched entity rows |
| `pull_bucket_files` | `false` | Sync files directly from subject folders in the workspace GCS bucket |
| `pull_workspace_bucket` | `false` | Mirror all blobs in the workspace `gs://fc-...` bucket under the target subject |
| `entity_types` | `null` | Optional list of Terra entity tables to pull (`null` pulls all tables) |
| `table_format` | `"json"` | Output format for table extracts: `"json"`, `"tsv"`, or `"csv"` |
| `subject_id_column` | `"name"` | Entity field/attribute matching subject ID (`"name"` or attribute column, including Terra entity references) |
| `subject_id_regex` | `null` | Optional regex (with optional capture group 1) to normalize subject IDs (e.g., stripping `'1071-ME04962'` $\rightarrow$ `'ME04962'`) |
| `fallback_subject_id` | `null` | Optional cohort subject ID for unmatched entity rows (e.g., `sample_set` or QC controls) |
| `subject_id_metadata_key` | `null` | Optional dot-path in Lochness `subject_metadata` (e.g., `"terra_id"` or `"terra.participant_id"`) |
| `gcs_uri_columns` | `null` | Optional list of table attribute columns to scan for `gs://` URIs (`null` scans all) |
| `bucket_name` | `null` | Optional GCS bucket override (defaults to workspace's `bucketName`) |
| `bucket_prefix` | `""` | Optional prefix inside the workspace GCS bucket |
| `bucket_subject_folder_template` | `"{subject_id}"` | Subfolder naming template under `bucket_prefix` |
| `include_patterns` | `null` | Optional list of glob patterns for GCS files to include (e.g., `["*.bam", "*.vcf.gz"]`) |
| `exclude_patterns` | `null` | Optional list of glob patterns for GCS files to exclude |
| `billing_project` | `null` | Optional GCP project for Requester Pays GCS buckets |
| `max_workers` | `16` | Parallel worker threads for GCS metadata/download operations |

## 3. Auto-Discover Workspaces, Sites & Subjects (Standalone Terra Deployment)

When deploying Lochness without REDCap/RPMS, `refresh_metadata.py` discovers non-public AMP-SCZ Terra workspaces and automatically populates `projects`, `key_store`, 2-letter `sites` + `COHORT`, `subjects`, and `data_sources`:

```bash
python lochness/sources/terra/tasks/refresh_metadata.py \
  --b2c-token-json ~/terra_b2c_token.json
```

## 4. Running a Pull

Dry-run first to verify connectivity and inspect planned downloads:

```bash
python lochness/sources/terra/tasks/pull_data.py \
  --project_id ProCAN \
  --site_id YA \
  --dry_run
```

Run the full pull:

```bash
python lochness/sources/terra/tasks/pull_data.py \
  --project_id ProCAN \
  --site_id YA
```

For scheduled execution in Airflow, enable the `lochness_terra_pull_data` DAG defined in `dags/terra_pull_data.py`.

## 5. PHOENIX Output Structure

Downloaded data is organized under:

```text
{lochness_root}/{Project}/PHOENIX/PROTECTED/{Project}{site_id}/raw/{subject_id}/{modality}/
├── tables/
│   └── {entity_type}_{subject_id}.{json|tsv|csv}
├── table_files/
│   └── {entity_type}/{gcs_bucket}/{blob_path}
└── bucket_files/
    └── {relative_blob_path}
```

Incremental sync uses GCS object hashes (`md5_hash`, falling back to `crc32c` for composite objects, then `generation`/`etag`) and SHA-256 table hashes stored both in local `.terraversion` sidecar files and in PostgreSQL `data_pull.pull_metadata` (so files removed locally after being pushed to a sink are not re-downloaded).
