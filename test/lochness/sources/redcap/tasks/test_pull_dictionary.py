import sys
import os
from pathlib import Path

for x in sys.path:
    if 'lochness' in x:
        sys.path.remove(x)

file = Path(__file__).resolve()
parent = file.parent
root_dir = None
for parent in file.parents:
    if parent.name == "lochness_v2":
        root_dir = parent

sys.path.append(str(root_dir))
sys.path.append(str(root_dir / 'test'))
from test_module import fake_data_fixture, config_file
from lochness.sources.redcap.models.data_source import (
        RedcapDataSourceMetadata,
        RedcapDataSource
        )
from lochness.sources.redcap.tasks.pull_dictionary import (
        fetch_dictionary,
        refresh_redcap_dictionary
        )
from lochness.models.subjects import Subject
from lochness.models.sites import Site
from lochness.models.projects import Project
from lochness.models.keystore import KeyStore
from lochness.models.data_source import DataSource
from lochness.helpers import logs, utils, db, config


def test_init():
    pass


def test_fetch_dictionary():
    # cd /Users/kc244/tmp/lochness_v2/lochness/scripts
    # PYTHONPATH=/Users/kc244/tmp/lochness_v2 python init_db.py
    # PYTHONPATH=/Users/kc244/tmp/lochness_v2 python pull_dictionary.py --project_id project_id --site_id site_id
    config_file = utils.get_config_file_path()
    encryption_passphrase = config.parse(config_file, 'general')[
            'encryption_passphrase']
    PROJECT_ID = 'project_id'
    SITE_ID = 'site_id'
    SITE_NAME = 'site_name'

    # create fake project
    project = Project(
            project_id=PROJECT_ID,
            project_name='project_name',
            project_is_active=True,
            project_metadata={'testing': True}
            )
    db.execute_queries(config_file, [project.to_sql_query()])

    # create fake site
    site = Site(
            site_id=SITE_ID,
            site_name=SITE_NAME,
            project_id=PROJECT_ID,
            site_is_active=True,
            site_metadata={'testing': True}
            )
    db.execute_queries(config_file, [site.to_sql_query()])

    redcap_cred = config.parse(config_file, 'redcap-test')
    # create key store the main redcap
    keystore = KeyStore(
            key_name=redcap_cred['key_name'],
            key_value=redcap_cred['key_value'],
            key_type=redcap_cred['key_type'],
            project_id=PROJECT_ID,
            key_metadata={})
    db.execute_queries(config_file,
                       [keystore.to_sql_query(encryption_passphrase)])

    # create fake data source for main redcap
    dataSource = DataSource(
            data_source_name=redcap_cred['data_source_name'],
            is_active=True,
            site_id=SITE_ID,
            project_id=PROJECT_ID,
            data_source_type='redcap',
            data_source_metadata={
                'testing': True,
                'modality': 'surveys',
                'keystore_name': redcap_cred['key_name'],
                'endpoint_url': redcap_cred['endpoint_url'],
                'subject_id_variable': redcap_cred['subject_id_variable'],
                'subject_id_variable_as_the_pk':
                    redcap_cred['subject_id_variable_as_the_pk'],
                'messy_subject_id':
                    redcap_cred['messy_subject_id'],
                'main_redcap': redcap_cred['main_redcap']
                }
            )
    db.execute_queries(config_file, [dataSource.to_sql_query()])

    redcap_cred = config.parse(config_file, 'redcap-test')
    data_source_name = redcap_cred['data_source_name']

    redcapDataSourceMetadata = RedcapDataSourceMetadata(
            keystore_name=redcap_cred['key_name'],
            endpoint_url=redcap_cred['endpoint_url'],
            subject_id_variable=redcap_cred['subject_id_variable'],
            optional_variables_dictionary=[],
            main_redcap=redcap_cred['main_redcap'])

    redcapDataSource = RedcapDataSource(
        data_source_name=data_source_name,
        is_active=True,
        site_id=SITE_ID,
        project_id=PROJECT_ID,
        data_source_type='redcap',
        data_source_metadata=redcapDataSourceMetadata)

    fetch_dictionary(redcapDataSource, config_file)
