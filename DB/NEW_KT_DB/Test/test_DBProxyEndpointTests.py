from typing import Dict
import pytest
from unittest.mock import Mock
from DB.NEW_KT_DB.DataAccess.ObjectManager import ObjectManager
from DB.NEW_KT_DB.DataAccess.DBManager import DBManager
from DB.NEW_KT_DB.DataAccess.DBProxyEndpointManager import DBProxyEndpointManager
from Storage.NEW_KT_Storage.DataAccess.StorageManager import StorageManager
from DB.NEW_KT_DB.Service.Classes.DBProxyEndpointService import DBProxyEndpointService
from DB.NEW_KT_DB.Service.Classes.DBProxyEndpointService import DBProxyNotFoundFault, DBProxyEndpointAlreadyExistsFault,DBProxyEndpointNotFoundFault, DBProxyEndpointQuotaExceededFault, ParamValidationFault
from DB.NEW_KT_DB.Controller.DBProxyEndpointController import DBProxyEndpointController
from DB.NEW_KT_DB.Models.DBProxyEndpointModel import DBProxyEndpoint
from DB.NEW_KT_DB.Test.GeneralTests import test_file_exists

@pytest.fixture
def storage_manager() -> StorageManager:
    storage_manager:StorageManager = StorageManager(':memory:')
    return storage_manager

@pytest.fixture
def object_manager() -> ObjectManager:
    object_manager:ObjectManager = ObjectManager(':memory:')
    return object_manager

@pytest.fixture
def endpoint_manager(object_manager:ObjectManager) -> DBProxyEndpointManager:
    endpoint_manager:DBProxyEndpointManager = DBProxyEndpointManager(object_manager)
    return endpoint_manager
    

@pytest.fixture
def endpoint_service(endpoint_manager:DBProxyEndpointManager, storage_manager:StorageManager) -> DBProxyEndpointService:
    endpoint_service:DBProxyEndpointService = DBProxyEndpointService(endpoint_manager, storage_manager)
    return endpoint_service
    

@pytest.fixture
def endpoint_controller(endpoint_service:DBProxyEndpointService) -> DBProxyEndpointController:
    controller:DBProxyEndpointController = DBProxyEndpointController(endpoint_service)
    return controller

@pytest.fixture
def setup_db_proxy_endpoint(endpoint_controller: DBProxyEndpointController):
    db_proxy_name = "my-proxy"
    endpoint_name = "my-endpoint"
    target_role = 'READ_WRITE'
    
    endpoint_description = endpoint_controller.create_db_proxy_endpoint(db_proxy_name, endpoint_name, target_role)
    
    return db_proxy_name, endpoint_name, target_role, endpoint_description

    


def _test_exists_in_db(endpoint_manager:DBProxyEndpointManager, endpoint_name):
    assert endpoint_manager.is_exists(endpoint_name)
   

def _test_file_content_is_correct(storage_manager:StorageManager,file_path, db_proxy_name, endpoint_name, target_role):
    pass
        

def _test_description_is_correct(description, db_proxy_name, endpoint_name, target_role):
    endpoint_data = description[DBProxyEndpoint.object_name]
    assert endpoint_data['DBProxyName'] == db_proxy_name
    assert endpoint_data['DBProxyEndpointName'] == endpoint_name
    assert endpoint_data['TargetRole'] == target_role
    
     
def test_create_db_proxy_endpoint(endpoint_service:DBProxyEndpointService, setup_db_proxy_endpoint):
    
    # Create and get response
    db_proxy_name, endpoint_name, target_role, endpoint = setup_db_proxy_endpoint
    
    # Check if response is correct
    # if endpoint is not None:
    #     endpoint_data = endpoint.get(DBProxyEndpoint.object_name)
    #     if endpoint_data is not None:
    #         assert 1 == 2
    #     else:
    #         assert 2 == 3
    # else:
    #     assert 3 == 4
    _test_description_is_correct(endpoint, db_proxy_name, endpoint_name, target_role)
    
    # Check if phisical file exists
    endpoint_file_name = endpoint_service._convert_endpoint_name_to_endpoint_file_name(endpoint_name)
    test_file_exists(endpoint_file_name)
    
    # Check if data in table
    _test_exists_in_db(endpoint_name)
    
    

def test_create_when_db_proxy_not_exist(endpoint_controller:DBProxyEndpointController):
    db_proxy_name = "not-exist-proxy"
    endpoint_name = "my-endpoint"
    target_role = 'READ_WRITE'
    
    # Create
    with pytest.raises(DBProxyNotFoundFault):
        endpoint_controller.create_db_proxy_endpoint(db_proxy_name, endpoint_name, target_role)
    

def test_create_db_proxy_endpoint_with_existing_name(setup_db_proxy_endpoint,endpoint_controller:DBProxyEndpointController):
    # Create first
    db_proxy_name, endpoint_name, target_role, endpoint = setup_db_proxy_endpoint
    # Create sec
    with pytest.raises(DBProxyEndpointAlreadyExistsFault):
        endpoint_controller.create_db_proxy_endpoint(db_proxy_name, endpoint_name, target_role)

def test_exceed_db_proxy_endpoints_quota(endpoint_controller:DBProxyEndpointController):
    db_proxy_name = "my-proxy"
    endpoint_name = "my-endpoint"
    for i in range(19):
        endpoint_name_now = endpoint_name + "_" + str(i)
        endpoint_controller.create_db_proxy_endpoint(db_proxy_name, endpoint_name_now)
    with pytest.raises(DBProxyEndpointQuotaExceededFault):
        endpoint_controller.create_db_proxy_endpoint(db_proxy_name, endpoint_name)

def test_delete_db_proxy_endpoint(setup_db_proxy_endpoint, endpoint_controller:DBProxyEndpointController):
     # Create and get response
    db_proxy_name, endpoint_name, target_role, endpoint = setup_db_proxy_endpoint
    
    # Delete
    endpoint_description = endpoint_controller.delete_db_proxy_endpoint(endpoint_name)
    
    # Check if response is correct
    _test_description_is_correct(endpoint_description, db_proxy_name, endpoint_name, target_role)
    
    # Check if phisical file not exists
    endpoint_file_name = endpoint_service._convert_endpoint_name_to_endpoint_file_name(endpoint_name)
    assert not test_file_exists(endpoint_file_name)
    
    # Check if data not in table
    assert not _test_exists_in_db(endpoint_name)
    

def test_delete_non_exist_db_proxy_endpoint(endpoint_controller:DBProxyEndpointController):
    endpoint_name = "non-exist-endpoint"
    
    # Delete
    with pytest.raises(DBProxyEndpointNotFoundFault):
        endpoint_controller.delete_db_proxy_endpoint(endpoint_name)

def test_delete_non_valid_state_db_proxy_endpoint(endpoint_controller:DBProxyEndpointController):
    db_proxy_name = "my-proxy"
    endpoint_name = "my-endpoint"
    target_role = 'READ_WRITE'
    endpoint = endpoint_controller.create_db_proxy_endpoint(db_proxy_name, endpoint_name, target_role)
    # await to functions to be async

def test_modify_name_to_db_proxy_endpoint(setup_db_proxy_endpoint,endpoint_service:DBProxyEndpointService,endpoint_controller:DBProxyEndpointController):
    
     # Create and get response
    db_proxy_name, endpoint_name, target_role, endpoint = setup_db_proxy_endpoint
    
    # Modify
    new_name = "your-endpoint"
    endpoint = endpoint_controller.modify_db_proxy_endpoint(endpoint_name, new_name)
    
    # Check if response is correct
    _test_description_is_correct(endpoint, db_proxy_name, new_name, target_role)
    
    # Check if phisical file is up-to-date
    old_endpoint_file_name = endpoint_service._convert_endpoint_name_to_endpoint_file_name(endpoint_name)
    new_endpoint_file_name = endpoint_service._convert_endpoint_name_to_endpoint_file_name(new_name)
    assert not test_file_exists(old_endpoint_file_name)
    test_file_exists(new_endpoint_file_name)
    
    # Check if data in table in updated
    _test_exists_in_db(new_name)
    assert not _test_exists_in_db(endpoint_name)
    
    

def test_modify_non_exist_db_proxy_endpoint(endpoint_controller:DBProxyEndpointController):
    endpoint_name = "non-exist-endpoint"
    new_name = "your-endpoint"
    with pytest.raises(DBProxyEndpointNotFoundFault):
        endpoint_controller.modify_db_proxy_endpoint(endpoint_name, new_name)
        

def test_describe_db_proxy_endpoint(setup_db_proxy_endpoint,endpoint_controller:DBProxyEndpointController):
     # Create and get response
    db_proxy_name, endpoint_name, target_role, endpoint = setup_db_proxy_endpoint
    
    # Describe
    description = endpoint_controller.describe_db_proxy_endpoint(endpoint_name)
    
    _test_description_is_correct(description, db_proxy_name, endpoint_name, target_role)

def test_describe_non_exist_db_proxy_endpoint(endpoint_controller:DBProxyEndpointController):
    endpoint_name = "non-exist-endpoint"
    with pytest.raises(DBProxyEndpointNotFoundFault):
        endpoint_controller.describe_db_proxy_endpoint(endpoint_name)


    
def test_describe_with_filters(setup_db_proxy_endpoint,endpoint_controller:DBProxyEndpointController):
    
     # Create and get response first endpoint
    db_proxy_name, endpoint_name, target_role, endpoint = setup_db_proxy_endpoint
    
    # Create sec
    endpoint_name2 = "my-endpoint2"
    target_role2 = 'READ_ONLY'
    
    endpoint_controller.create_db_proxy_endpoint(db_proxy_name, endpoint_name2, target_role2)
    
    # Describe
    filters = [
        {
            'Name': 'target_role',
            'Values': [
                'READ_WRITE'
            ]
        }
    ]
    description = endpoint_controller.describe_db_proxy_endpoint(endpoint_name, filters)
    endpoint_data = description[DBProxyEndpoint.object_name]
    assert any(d.get('TargetRole') == target_role for d in endpoint_data)
    assert all(d.get('TargetRole') != target_role2 for d in endpoint_data)

def test_describe_with_filters_does_nothing(setup_db_proxy_endpoint, endpoint_controller:DBProxyEndpointController):
     # Create and get response
    db_proxy_name, endpoint_name, target_role, endpoint = setup_db_proxy_endpoint
    # Describe
    filters = [{'Name': 'yyy','Values':['hh','jj']},
               {'Name': 'TargetRole','Values':['hh','jj']}]
    description = endpoint_controller.describe_db_proxy_endpoint(endpoint_name, filters)
    _test_description_is_correct(description, db_proxy_name, endpoint_name, target_role)

def test_describe_with_non_correct_filters(setup_db_proxy_endpoint,endpoint_controller:DBProxyEndpointController):
     # Create and get response
    db_proxy_name, endpoint_name, target_role, endpoint = setup_db_proxy_endpoint
    
    def test_filters(filters):
        nonlocal endpoint_controller
        nonlocal endpoint_name
        with pytest.raises(ParamValidationFault):
            endpoint_controller.describe_db_proxy_endpoint(endpoint_name, filters)
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
    

    
