import sys
import os
import hashlib
import time
from pathlib import Path
import json

# Add project root to Python path
file = Path(__file__).resolve()
parent = file.parent
root_dir = None
for p in file.parents:
    if p.name == 'lochness_v2':
        root_dir = p
if root_dir:
    sys.path.append(str(root_dir))
else:
    # If running from project root, add current dir
    sys.path.append(str(parent))

from lochness.helpers import utils, db, config
from lochness.models.data_source import DataSource
from lochness.models.data_pulls import DataPull
from lochness.models.files import File
from lochness.sources.sharepoint.tasks.sync import get_access_token # For real integration

# Test Data Source Name (must match what's in your data_sources table)
TEST_DATA_SOURCE_NAME = "TestSharePointDS"
TEST_SITE_ID = "TestSite"
TEST_PROJECT_ID = "ProCAN"
TEST_SUBJECT_ID = "SUB001"

# Dummy file details (simulating a downloaded file)
TEST_FILE_NAME = "simulated_eeg_data.csv"
TEST_FILE_CONTENT = b"subject_id,eeg_value\nSUB001,123.45\n"

def calculate_md5(file_path: Path) -> str:
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

def main():
    print(f"Attempting to simulate data pull from SharePoint for subject '{TEST_SUBJECT_ID}'...")

    config_file = utils.get_config_file_path()
    if not config_file.exists():
        print(f"ERROR: Configuration file not found at {config_file}")
        print("Please ensure 'config.ini' exists in the project root.")
        return

    # 1. Create a dummy file (simulating a downloaded file)
    test_file_path = Path("/tmp") / TEST_FILE_NAME
    with open(test_file_path, "wb") as f:
        f.write(TEST_FILE_CONTENT)
    print(f"Created dummy simulated downloaded file: {test_file_path}")
    file_md5 = calculate_md5(test_file_path)

    try:
        # 2. Ensure the files table exists
        file_obj_init = File(file_path=test_file_path) # Just to get the init query
        db.execute_queries(config_file, [file_obj_init.init_db_table_query()], show_commands=False)
        print("Files table initialized (if not already present).")

        # 3. Ensure the data_pull table exists
        data_pull_init = DataPull(
            subject_id=TEST_SUBJECT_ID,
            data_source_name=TEST_DATA_SOURCE_NAME,
            site_id=TEST_SITE_ID,
            project_id=TEST_PROJECT_ID,
            file_path=str(test_file_path),
            file_md5=file_md5,
            pull_time_s=0, # Placeholder
            pull_metadata={}
        )
        db.execute_queries(config_file, [data_pull_init.init_db_table_query()], show_commands=False)
        print("Data pull table initialized (if not already present).")

        # 4. Directly define Data Source metadata for testing
        data_source_metadata = {
            "keystore_name": "sharepoint_prod", # Assuming this is set up
            "site_url": "https://yourcompany.sharepoint.com/sites/TestTeam", # Placeholder
            "form_id": "00000000-0000-0000-0000-000000000000", # Placeholder
        }

        # For a real pull, you would use data_source_metadata to connect to SharePoint
        # and download the file. For this test, we're simulating the download.

        # 5. Record file metadata in the 'files' table
        file_obj = File(
            file_path=test_file_path,
        )
        db.execute_queries(config_file, [file_obj.to_sql_query()], show_commands=False)
        print("File record inserted/updated in 'files' table.")

        # 6. Record data pull in DB
        start_time = time.time() # Simulate pull time
        # In a real scenario, this would be the actual time taken for download
        pull_time_s = int(time.time() - start_time)

        data_pull = DataPull(
            subject_id=TEST_SUBJECT_ID,
            data_source_name=TEST_DATA_SOURCE_NAME,
            site_id=TEST_SITE_ID,
            project_id=TEST_PROJECT_ID,
            file_path=str(test_file_path),
            file_md5=file_md5,
            pull_time_s=pull_time_s,
            pull_metadata={
                "simulated_source_path": "SharePoint/Forms/EEG/SUB001/data.csv",
                "simulated_download_status": "success",
            },
        )
        db.execute_queries(config_file, [data_pull.to_sql_query()], show_commands=False)
        print("Data pull record inserted into 'data_pull' table.")

    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # Clean up dummy file
        if test_file_path.exists():
            os.remove(test_file_path)
            print(f"Cleaned up dummy file: {test_file_path}")


def test_import():
    pass


if __name__ == "__main__":
    main()
