from typing import Dict, Literal, Optional
from unittest.mock import MagicMock
import pytest
import os
from DB.NEW_KT_DB.DataAccess.ObjectManager import ObjectManager
from DB.NEW_KT_DB.DataAccess.DBManager import DBManager
from DB.NEW_KT_DB.DataAccess.DBProxyEndpointManager import DBProxyEndpointManager
from Storage.NEW_KT_Storage.DataAccess.StorageManager import StorageManager
from DB.NEW_KT_DB.Service.Classes.DBProxyEndpointService import DBProxyEndpointService
from DB.NEW_KT_DB.Exceptions.DBProxyEndpointExceptions import *
from DB.NEW_KT_DB.Exceptions.GeneralExeptions import InvalidParamException
from DB.NEW_KT_DB.Controller.DBProxyEndpointController import DBProxyEndpointController
from DB.NEW_KT_DB.Models.DBProxyEndpointModel import DBProxyEndpoint
from DB.NEW_KT_DB.Test.GeneralTests import is_file_exist

# Infrastructure to tests
@pytest.fixture
def storage_manager() -> StorageManager:
    storage_manager:StorageManager = StorageManager('DBProxyEndpoints')
    return storage_manager


@pytest.fixture
def object_manager(): 
    object_manager:ObjectManager = ObjectManager('test.db')
    yield object_manager
    if os.path.exists('test.db'):
        os.remove('test.db')
    


@pytest.fixture
def endpoint_manager(object_manager:ObjectManager) -> DBProxyEndpointManager:
    endpoint_manager:DBProxyEndpointManager = DBProxyEndpointManager(object_manager)
    return endpoint_manager
    

@pytest.fixture
def endpoint_service(endpoint_manager:DBProxyEndpointManager, storage_manager:StorageManager, db_proxy_service_mock) -> DBProxyEndpointService:
    endpoint_service:DBProxyEndpointService = DBProxyEndpointService(endpoint_manager, storage_manager, db_proxy_service_mock)
    return endpoint_service


@pytest.fixture
def db_proxy_service_mock():
    mock = MagicMock()
    mock.is_exists.side_effect = lambda db_proxy_name: db_proxy_name == 'my-proxy'
    return mock   


@pytest.fixture
def endpoint_controller(endpoint_service:DBProxyEndpointService) -> DBProxyEndpointController:
    controller:DBProxyEndpointController = DBProxyEndpointController(endpoint_service)
    return controller


@pytest.fixture
def setup_db_proxy_endpoint(endpoint_controller: DBProxyEndpointController):
    """Create a db proxy endpoint for tests and delete it after tests"""
    db_proxy_name = "my-proxy"
    endpoint_name = "my-endpoint"
    vpc_subnet_ids = ['subnet-12345678', 'subnet-87654321']
    target_role = 'READ_WRITE'
    
    endpoint_description = endpoint_controller.create_db_proxy_endpoint(db_proxy_name, endpoint_name,vpc_subnet_ids, TargetRole=target_role)
    
    yield db_proxy_name, endpoint_name, vpc_subnet_ids, target_role, endpoint_description
    
    # Perform cleanup after the test has completed
    try:
        endpoint_controller.delete_db_proxy_endpoint(endpoint_name)
    except:
        print("alraedy deleted")


@pytest.fixture
def cleanup_endpoint(endpoint_controller: DBProxyEndpointController):
    """Delete an endpoint that get as parameter from test"""
    endpoint_to_clean:Optional[str] = None
    def set_endpoint_to_clean(endpoint_name: str):
        nonlocal endpoint_to_clean
        endpoint_to_clean = endpoint_name

    yield set_endpoint_to_clean
    if endpoint_to_clean:
        try:
            endpoint_controller.delete_db_proxy_endpoint(endpoint_to_clean)
        except:
            print("alraedy deleted")
    

# Tests    
    
    
def _test_exists_in_db(endpoint_manager:DBProxyEndpointManager, endpoint_name):
    """Cecking if endpoint endpoint_name exists in dd"""
    return endpoint_manager.is_exists(endpoint_name)


def _test_description_is_correct(description, db_proxy_name, endpoint_name, vpc_subnet_ids, target_role):
    """Checking if function response describe dbProxyEndpoint object correctly"""
    endpoint_data = description[DBProxyEndpoint.object_name][0]
    assert endpoint_data['DBProxyName'] == db_proxy_name
    assert endpoint_data['DBProxyEndpointName'] == endpoint_name
    assert endpoint_data['VpcSubnetIds'] == vpc_subnet_ids
    assert endpoint_data['TargetRole'] == target_role
    
     
def test_create_db_proxy_endpoint(endpoint_service:DBProxyEndpointService, setup_db_proxy_endpoint: tuple[Literal['my-proxy'], Literal['my-endpoint'], Literal['READ_WRITE'], dict[str, list[dict]]], storage_manager:StorageManager,
                                  endpoint_manager:DBProxyEndpointManager):
    
    # Create and get response
    db_proxy_name, endpoint_name, vpc_subnet_ids, target_role, endpoint = setup_db_proxy_endpoint
    
    # Check if response is correct
    _test_description_is_correct(endpoint, db_proxy_name, endpoint_name, vpc_subnet_ids, target_role)
    
    # Check if phisical file exists
    endpoint_file_name = endpoint_service._convert_endpoint_name_to_endpoint_file_name(endpoint_name)
    assert is_file_exist(storage_manager, endpoint_file_name)
    
    # Check if data in table
    _test_exists_in_db(endpoint_manager, endpoint_name)
    

def test_create_with_non_valid_parameters(endpoint_controller:DBProxyEndpointController) :
    valid_params = ["my-proxy","my-endpoint",['subnet-12345678', 'subnet-87654321'],'READ_WRITE',[{ 'Key': 'string','Value': 'string'}]]
    non_valid_params = ["my_proxy","my_en455", ['subnet-12345678', 'subnet-87654321'], "ff",{'Key': 1,'Value': 'string'}] 
    with pytest.raises(InvalidParamException):
        endpoint_controller.create_db_proxy_endpoint(valid_params[0], non_valid_params[1], valid_params[2],valid_params[3], valid_params[4])

    with pytest.raises(InvalidParamException):
        endpoint_controller.create_db_proxy_endpoint(valid_params[0], valid_params[1], valid_params[2], non_valid_params[3], valid_params[4])

    with pytest.raises(InvalidParamException):
        endpoint_controller.create_db_proxy_endpoint(non_valid_params[0], valid_params[1], valid_params[2],valid_params[3], valid_params[4])
        
    with pytest.raises(InvalidParamException):
        endpoint_controller.create_db_proxy_endpoint(valid_params[0], valid_params[1], valid_params[2],valid_params[3], non_valid_params[4])
        

def test_create_when_db_proxy_not_exist(endpoint_controller:DBProxyEndpointController):
    db_proxy_name = "not-exist-proxy"
    endpoint_name = "my-endpoint"
    vpc_subnet_ids = ['subnet-12345678', 'subnet-87654321']
    target_role = 'READ_WRITE'
    
    # Create
    with pytest.raises(DBProxyNotFoundException):
        endpoint_controller.create_db_proxy_endpoint(db_proxy_name, endpoint_name, vpc_subnet_ids, target_role)
    

def test_create_db_proxy_endpoint_with_existing_name(setup_db_proxy_endpoint: tuple[Literal['my-proxy'], Literal['my-endpoint'], Literal['READ_WRITE'], dict[str, list[dict]]],endpoint_controller:DBProxyEndpointController):
    # Create first
    db_proxy_name, endpoint_name, vpc_subnet_ids, target_role, endpoint = setup_db_proxy_endpoint
    # Create sec
    with pytest.raises(DBProxyEndpointAlreadyExistsException):
        endpoint_controller.create_db_proxy_endpoint(db_proxy_name, endpoint_name, vpc_subnet_ids, target_role)


def test_delete_db_proxy_endpoint(setup_db_proxy_endpoint: tuple[Literal['my-proxy'], Literal['my-endpoint'], Literal['READ_WRITE'], dict[str, list[dict]]], endpoint_controller:DBProxyEndpointController,
                                  storage_manager:StorageManager, endpoint_manager:DBProxyEndpointManager, 
                                  endpoint_service:DBProxyEndpointService):
     # Create and get response
    db_proxy_name, endpoint_name, vpc_subnet_ids, target_role, endpoint = setup_db_proxy_endpoint
    
    # Delete
    endpoint_description = endpoint_controller.delete_db_proxy_endpoint(endpoint_name)
    
    # Check if response is correct
    _test_description_is_correct(endpoint_description, db_proxy_name, endpoint_name, vpc_subnet_ids, target_role)
    
    # Check if phisical file not exists
    endpoint_file_name = endpoint_service._convert_endpoint_name_to_endpoint_file_name(endpoint_name)
    assert not is_file_exist(storage_manager, endpoint_file_name)
    
    # Check if data not in table
    assert not _test_exists_in_db(endpoint_manager, endpoint_name)
    

def test_delete_non_exist_db_proxy_endpoint(endpoint_controller:DBProxyEndpointController):
    endpoint_name = "non-exist-endpoint"
    
    # Delete
    with pytest.raises(DBProxyEndpointNotFoundException):
        endpoint_controller.delete_db_proxy_endpoint(endpoint_name)

def test_delete_non_valid_state_db_proxy_endpoint(endpoint_controller:DBProxyEndpointController, endpoint_service:DBProxyEndpointService,
                                                  setup_db_proxy_endpoint):
    db_proxy_name, endpoint_name, vpc_subnet_ids, target_role, endpoint = setup_db_proxy_endpoint
    state = 'not available'
    endpoint_service.modify(endpoint_name,  Status= state)
    with pytest.raises(InvalidDBProxyEndpointStateException):
        endpoint_controller.delete_db_proxy_endpoint(endpoint_name)


def test_modify_name_to_db_proxy_endpoint(setup_db_proxy_endpoint: tuple[Literal['my-proxy'], Literal['my-endpoint'], Literal['READ_WRITE'], 
                                                                         dict[str, list[dict]]],endpoint_service:DBProxyEndpointService,
                                          endpoint_controller:DBProxyEndpointController,
                                          storage_manager:StorageManager, endpoint_manager:DBProxyEndpointManager,
                                          cleanup_endpoint:Optional[str]):
    
     # Create and get response
    db_proxy_name, endpoint_name, vpc_subnet_ids, target_role, endpoint = setup_db_proxy_endpoint
    
    # Modify
    new_name = "your-endpoint"
    endpoint = endpoint_controller.modify_db_proxy_endpoint(endpoint_name, new_name)
    
    # Check if response is correct
    _test_description_is_correct(endpoint, db_proxy_name, new_name, vpc_subnet_ids, target_role)
    
    # Check if phisical file is up-to-date
    old_endpoint_file_name = endpoint_service._convert_endpoint_name_to_endpoint_file_name(endpoint_name)
    new_endpoint_file_name = endpoint_service._convert_endpoint_name_to_endpoint_file_name(new_name)
    assert not is_file_exist(storage_manager, old_endpoint_file_name)
    assert is_file_exist(storage_manager, new_endpoint_file_name)
    
    # Check if data in table in updated
    assert _test_exists_in_db(endpoint_manager, new_name)
    assert not _test_exists_in_db(endpoint_manager, endpoint_name)
    
    # Clean the modified endpoint
    cleanup_endpoint(new_name)
    

def test_modify_non_exist_db_proxy_endpoint(endpoint_controller:DBProxyEndpointController):
    endpoint_name = "non-exist-endpoint"
    new_name = "your-endpoint"
    with pytest.raises(DBProxyEndpointNotFoundException):
        endpoint_controller.modify_db_proxy_endpoint(endpoint_name, new_name)
        

def test_describe_db_proxy_endpoint(setup_db_proxy_endpoint: tuple[Literal['my-proxy'], Literal['my-endpoint'], Literal['READ_WRITE'], dict[str, list[dict]]],endpoint_controller:DBProxyEndpointController):
     # Create and get response
    db_proxy_name, endpoint_name, vpc_subnet_ids, target_role, endpoint = setup_db_proxy_endpoint
    
    # Describe
    description = endpoint_controller.describe_db_proxy_endpoint(endpoint_name)
    
    _test_description_is_correct(description, db_proxy_name, endpoint_name, vpc_subnet_ids, target_role)


def test_describe_non_exist_db_proxy_endpoint(endpoint_controller:DBProxyEndpointController):
    endpoint_name = "non-exist-endpoint"
    with pytest.raises(DBProxyEndpointNotFoundException):
        endpoint_controller.describe_db_proxy_endpoint(DBProxyEndpointName=endpoint_name)


def test_describe_with_filters(setup_db_proxy_endpoint: tuple[Literal['my-proxy'], Literal['my-endpoint'], Literal['READ_WRITE'], dict[str, list[dict]]],endpoint_controller:DBProxyEndpointController,
                               cleanup_endpoint:Optional[str]):
    
     # Create and get response first endpoint
    db_proxy_name, endpoint_name, vpc_subnet_ids, target_role, endpoint = setup_db_proxy_endpoint
    
    # Create sec
    endpoint_name2 = "my-endpoint2"
    vpc_subnet_ids2 = ['subnet-12345678']
    target_role2 = 'READ_ONLY'
    
    endpoint_controller.create_db_proxy_endpoint(db_proxy_name, endpoint_name2, vpc_subnet_ids2, TargetRole=target_role2)
    
    # Describe
    filters = [
        {
            'Name': 'TargetRole',
            'Values': [
                'READ_WRITE'
            ]
        }
    ]
    description = endpoint_controller.describe_db_proxy_endpoint(Filters=filters)
    
    # Assert
    endpoint_data = description[DBProxyEndpoint.object_name]
    assert any(d.get('TargetRole') == target_role for d in endpoint_data)
    assert all(d.get('TargetRole') != target_role2 for d in endpoint_data)
    
    # Clean up the endpoints
    cleanup_endpoint(endpoint_name2)


def test_describe_with_filters_does_nothing(setup_db_proxy_endpoint: tuple[Literal['my-proxy'], Literal['my-endpoint'], Literal['READ_WRITE'], dict[str, list[dict]]], endpoint_controller:DBProxyEndpointController):
     # Create and get response
    db_proxy_name, endpoint_name, vpc_subnet_ids, target_role, endpoint = setup_db_proxy_endpoint
    # Describe
    filters = [{'Name': 'yyy','Values':['hh','jj']},
               {'Name': 'TargetRol','Values':['hh','jj']}]
    description = endpoint_controller.describe_db_proxy_endpoint(DBProxyEndpointName=endpoint_name, Filters=filters)
    _test_description_is_correct(description, db_proxy_name, endpoint_name, vpc_subnet_ids, target_role)


def test_describe_with_non_correct_filters(setup_db_proxy_endpoint: tuple[Literal['my-proxy'], Literal['my-endpoint'], Literal['READ_WRITE'], dict[str, list[dict]]],endpoint_controller:DBProxyEndpointController):
     # Create and get response
    db_proxy_name, endpoint_name, vpc_subnet_ids, target_role, endpoint = setup_db_proxy_endpoint
    
    def test_filters(filters):
        nonlocal endpoint_controller
        nonlocal endpoint_name
        with pytest.raises(InvalidParamException):
            endpoint_controller.describe_db_proxy_endpoint(DBProxyEndpointName=endpoint_name, Filters=filters)
            
    # str filters
    test_filters("ddd")
    # One dict of filter and not arr of dicts
    test_filters({'Name': 'TargetRole','Values': ['READ_WRITE']})
    # Name int not str
    test_filters([{'Name': 1,'Values': ['READ_WRITE']}])
    # Values arr[int] and not arr[str]
    test_filters([{'Name': 'TargetRole','Values': [1,2,3]}])
    # Values str not list
    test_filters([{'Name': 'TargetRole','Values': 'hhh'}])
    # without values
    test_filters([{'Name': 'TargetRole','Val': ['READ_WRITE']}])
    # Without name
    test_filters([{'n': 'TargetRole','Val': ['READ_WRITE']}])
    

    
