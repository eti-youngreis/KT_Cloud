import os
import sys
import pytest
import sqlite3
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))

from Storage.NEW_KT_Storage.DataAccess.StorageManager import StorageManager
from DB.NEW_KT_DB.Service.Classes.DBClusterService import DBClusterService
from DB.NEW_KT_DB.DataAccess.DBClusterManager import DBClusterManager
from Controller.DBClusterController import DBClusterController
from DataAccess.ObjectManager import ObjectManager
from Service.Classes.DBInstanceService import DBInstanceManager,DBInstanceService,AlreadyExistsError,ParamValidationError,DBInstanceNotFoundError
from Exception.exception import MissingRequireParamError
from Controller.DBInstanceController import DBInstanceController

@pytest.fixture
def object_manager():
    return ObjectManager('Clusters/instances.db')

@pytest.fixture
def db_instance_manager(object_manager):
    return DBInstanceManager(object_manager)

@pytest.fixture
def db_instance_service(db_instance_manager):
    return DBInstanceService(db_instance_manager)

# @pytest.fixture
# def snapshot_service(object_manager):
#     return SnapShotService(SnapShotManager(object_manager))

@pytest.fixture
def db_instance_controller(db_instance_service):
    return DBInstanceController(db_instance_service)

@pytest.fixture
def db_cluster_manager():
    return DBClusterManager('Clusters/clusters.db')

@pytest.fixture
def storage_manager():
    return StorageManager('Clusters')

@pytest.fixture
def db_cluster_service(db_cluster_manager,storage_manager):
    return DBClusterService(db_cluster_manager,storage_manager, 'Clusters')

@pytest.fixture
def db_cluster_controller(db_cluster_service, db_instance_controller):
    return DBClusterController(db_cluster_service, db_instance_controller)

@pytest.fixture
def db_cluster_controller_with_cleanup(db_cluster_controller):
 
    # Return the controller, but ensure cleanup happens after the test
    yield db_cluster_controller
    
    # Finalizer to clean up after the test
    db_cluster_controller.delete_db_cluster('ClusterTest')

def test_create_cluster_works(db_cluster_controller_with_cleanup):
    cluster_data = {
        'db_cluster_identifier': 'ClusterTest',
        'engine': 'mysql',
        'allocated_storage':5,
        'db_subnet_group_name': 'my-subnet-group'
        }

    res = db_cluster_controller_with_cleanup.create_db_cluster(**cluster_data)
    # db_cluster_controller.delete_db_cluster('Cluster21')
    assert res == None

def test_create_cluster_missing_required_fields(db_cluster_controller):
    cluster_data = {
        'db_cluster_identifier': 'ClusterTest',
        'allocated_storage':5,
        'db_subnet_group_name': 'my-subnet-group'
        }
    with pytest.raises(ValueError):
           db_cluster_controller.create_db_cluster(**cluster_data)

def test_create_cluster_invalide_identifier(db_cluster_controller):
    cluster_data = {
        'db_cluster_identifier': 'Cluster__Test',
        'engine': 'mysql',
        'allocated_storage':5,
        'db_subnet_group_name': 'my-subnet-group'
        }
    with pytest.raises(ValueError):
           db_cluster_controller.create_db_cluster(**cluster_data)


def test_create_cluster_already_exist(db_cluster_controller):
    cluster_data = {
        'db_cluster_identifier': 'ClusterTest',
        'engine': 'mysql',
        'allocated_storage':5,
        'db_subnet_group_name': 'my-subnet-group'
        }
    db_cluster_controller.create_db_cluster(**cluster_data)
    with pytest.raises(ValueError):
           db_cluster_controller.create_db_cluster(**cluster_data)

    db_cluster_controller.delete_db_cluster('ClusterTest')
           


def test_delete_cluster_works(db_cluster_controller):
    cluster_data = {
        'db_cluster_identifier': 'ClusterTest',
        'engine': 'mysql',
        'allocated_storage':5,
        'db_subnet_group_name': 'my-subnet-group'
        }

    db_cluster_controller.create_db_cluster(**cluster_data)
    db_cluster_controller.delete_db_cluster('ClusterTest')
    with pytest.raises(ValueError):
        db_cluster_controller.delete_db_cluster('ClusterTest')
        

def test_delete_cluster_does_not_exist(db_cluster_controller):
    with pytest.raises(ValueError):
        db_cluster_controller.delete_db_cluster('ClusterTest')



def test_describe_cluster_works(db_cluster_controller_with_cleanup):
    cluster_data = {
        'db_cluster_identifier': 'ClusterTest',
        'engine': 'mysql',
        'allocated_storage':5,
        'db_subnet_group_name': 'my-subnet-group'
        }

    db_cluster_controller_with_cleanup.create_db_cluster(**cluster_data)
    res = db_cluster_controller_with_cleanup.describe_db_cluster('ClusterTest')
    assert isinstance(res, list)

def test_describe_cluster_does_not_exist(db_cluster_controller):
    with pytest.raises(ValueError):
        db_cluster_controller.describe_db_cluster('ClusterTest')


def test_modify_cluster_works(db_cluster_controller_with_cleanup):
    cluster_data = {
        'db_cluster_identifier': 'ClusterTest',
        'engine': 'mysql',
        'allocated_storage':5,
        'db_subnet_group_name': 'my-subnet-group'
        }

    db_cluster_controller_with_cleanup.create_db_cluster(**cluster_data)
    update_data = {
        'engine': 'postgres',
        }
    db_cluster_controller_with_cleanup.modify_db_cluster('ClusterTest', **update_data)
    res = db_cluster_controller_with_cleanup.describe_db_cluster('ClusterTest')
    columns = ['db_cluster_identifier', 'engine', 'allocated_storage', 'copy_tags_to_snapshot',
            'db_cluster_instance_class', 'database_name', 'db_cluster_parameter_group_name',
            'db_subnet_group_name', 'deletion_protection', 'engine_version', 'master_username',
            'master_user_password', 'manage_master_user_password', 'option_group_name', 'port',
            'replication_source_identifier', 'storage_encrypted', 'storage_type', 'tags',
            'created_at', 'status', 'primary_writer_instance', 'reader_instances', 'cluster_endpoint',
            'instances_endpoints', 'pk_column', 'pk_value']

    cluster_dict = dict(zip(columns, res[0]))
    assert cluster_dict['engine'] == 'postgres'

def test_modify_cluster_does_not_exist(db_cluster_controller):
    update_data = {
        'engine': 'postgres',
        }
    with pytest.raises(ValueError):
        db_cluster_controller.modify_db_cluster('ClusterTest',**update_data)


def test_modify_cluster_invalide_engine(db_cluster_controller_with_cleanup):
    cluster_data = {
        'db_cluster_identifier': 'ClusterTest',
        'engine': 'mysql',
        'allocated_storage':5,
        'db_subnet_group_name': 'my-subnet-group'
        }

    db_cluster_controller_with_cleanup.create_db_cluster(**cluster_data)
    update_data = {
        'engine': 'invalid',
        }
    with pytest.raises(ValueError):
        db_cluster_controller_with_cleanup.modify_db_cluster('ClusterTest',**update_data)

def test_get_all_clusters(db_cluster_controller_with_cleanup):
    cluster_data = {
    'db_cluster_identifier': 'ClusterTest',
    'engine': 'mysql',
    'allocated_storage':5,
    'db_subnet_group_name': 'my-subnet-group'
    }

    db_cluster_controller_with_cleanup.create_db_cluster(**cluster_data)
    res = db_cluster_controller_with_cleanup.get_all_db_clusters()
    assert isinstance(res, list)















