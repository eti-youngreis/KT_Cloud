import tempfile
import pytest
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))
from Storage.NEW_KT_Storage.Controller.BucketPolicyController import BucketPolicyController
from Storage.NEW_KT_Storage.Service.Classes.BucketPolicyService import BucketPolicyService, ParamValidationFault, IsExistactionFault, IsNotExistFault, IsNotExistactionFault 
from Storage.NEW_KT_Storage.DataAccess.BucketPolicyManager import BucketPolicyManager
from Storage.NEW_KT_Storage.DataAccess.StorageManager import StorageManager
from Storage.NEW_KT_Storage.DataAccess.ObjectManager import ObjectManager
from Storage.NEW_KT_Storage.Models.BucketPolicyModel import BucketPolicy

# @pytest.fixture
# def storage_manager() -> StorageManager:
#     return StorageManager(":memory:")

# @pytest.fixture
# def object_manager() -> ObjectManager:
#     return ObjectManager(":memory:")

@pytest.fixture
def bucketPolicy_manager() -> BucketPolicyManager:
    """Create a BucketService object for the tests with a temporary place"""
    bucketPolicy_manager = BucketPolicyManager()
    # with tempfile.TemporaryDirectory() as tmpdir:
    #     bucketPolicy_manager.storage_manager = StorageManager(tmpdir)
        
    return bucketPolicy_manager

@pytest.fixture
def bucketPolicy_service(bucketPolicy_manager:BucketPolicyManager):
    return BucketPolicyService(bucketPolicy_manager)

@pytest.fixture
def bucketPolicy_controller(bucketPolicy_service:BucketPolicyService):
    return BucketPolicyController(bucketPolicy_service)

@pytest.fixture
def setup_bucket_policy(bucketPolicy_controller:BucketPolicyController):
    bucket_name = "bucket"
    actions = ['READ']
    allow_versions = True
    
    bucket_policy = bucketPolicy_controller.create_bucket_policy(bucket_name, actions, allow_versions)
    return bucket_name, actions, allow_versions, bucket_policy

def _test_is_description_is_correct(description, bucket_name, actions, allow_versions):
    
    assert description['bucket_name'] == bucket_name
    for action in actions:
        assert action in description['actions']
    assert description['allow_versions'] == allow_versions

def test_create_bucket_policy_success(bucketPolicy_controller, setup_bucket_policy):
    
    bucket_name, actions, allow_versions, bucket_policy = setup_bucket_policy
    
    _test_is_description_is_correct(bucketPolicy_controller.get_bucket_policy(bucket_name), bucket_name, actions, allow_versions)
    
    bucketPolicy_controller.delete_bucket_policy(bucket_name)

    

def test_create_without_bucket_name(bucketPolicy_controller):
    
    with pytest.raises(ParamValidationFault):
        bucketPolicy_controller.create_bucket_policy()
        
def test_create_invalid_bucket_name(bucketPolicy_controller):
    
    with pytest.raises(ParamValidationFault):
        bucketPolicy_controller.create_bucket_policy(bucket_name=123)
        
def test_create_inavlid_action(bucketPolicy_controller):
    
    with pytest.raises(ParamValidationFault):
        bucketPolicy_controller.create_bucket_policy(bucket_name="my_bucket", actions="yyy")
        
def test_create_invalud_allow_versions(bucketPolicy_controller):
    
    with pytest.raises(ParamValidationFault):
        bucketPolicy_controller.create_bucket_policy(bucket_name="my_bucket", allow_versions="yes")
        
def test_add_action_to_bucket_policy(bucketPolicy_controller, setup_bucket_policy):
    
    bucket_name, actions, allow_versions, bucket_policy = setup_bucket_policy
    update_actions=['WRITE']
    bucketPolicy_controller.modify_bucket_policy("bucket", update_actions = update_actions, action="add")
    bucket_policy = bucketPolicy_controller.get_bucket_policy(bucket_name)
    _test_is_description_is_correct(bucket_policy, bucket_name, update_actions, allow_versions)
    
    bucketPolicy_controller.delete_bucket_policy(bucket_name)

    
def test_delete_action_from_bucket_policy(bucketPolicy_controller, setup_bucket_policy):
    
    bucket_name, actions, allow_versions, bucket_policy = setup_bucket_policy
    update_actions=['WRITE']
    bucketPolicy_controller.modify_bucket_policy(bucket_name, update_actions = update_actions, action="add")
    bucketPolicy_controller.modify_bucket_policy(bucket_name, action="delete", update_actions=['WRITE'])
    bucketPolicy_controller.delete_bucket_policy(bucket_name)

    # bucket_policy = bucketPolicy_controller.get_bucket_policy(bucket_name)
    # _test_is_description_is_correct(bucket_policy, bucket_name, update_actions, allow_versions)
    
def test_add_exist_action(bucketPolicy_controller):
    
    bucketPolicy_controller.create_bucket_policy("bucket", ['READ'])
    with pytest.raises(IsExistactionFault):
        bucketPolicy_controller.modify_bucket_policy("bucket", action="add", update_actions=['READ']) 
    bucketPolicy_controller.delete_bucket_policy("bucket")

def test_delete_not_exist_action(bucketPolicy_controller):
    
    bucketPolicy_controller.create_bucket_policy("my_bucket", ['READ'])
    with pytest.raises(IsNotExistactionFault):
        bucketPolicy_controller.modify_bucket_policy("my_bucket", action="delete", update_actions=['WRITE'])
    
def test_modify_bucket_policy(bucketPolicy_controller, setup_bucket_policy):
    
    bucket_name, actions, allow_versions, bucket_policy = setup_bucket_policy
    with pytest.raises(ParamValidationFault):
        bucketPolicy_controller.modify_bucket_policy(bucket_name, update_actions=['WRITE'])
    bucketPolicy_controller.delete_bucket_policy(bucket_name)


def test_modify_when_allow_versions_invalid(bucketPolicy_controller, setup_bucket_policy):
    
    bucket_name, actions, allow_versions, bucket_policy = setup_bucket_policy
    with pytest.raises(ParamValidationFault):
        bucketPolicy_controller.modify_bucket_policy(bucket_name, allow_versions="")
    bucketPolicy_controller.delete_bucket_policy(bucket_name)

        
def test_modify_invalid_action(bucketPolicy_controller, setup_bucket_policy):
    
    bucket_name, actions, allow_versions, bucket_policy = setup_bucket_policy
    with pytest.raises(ParamValidationFault):
        bucketPolicy_controller.modify_bucket_policy(bucket_name, action="add", update_actions="READ")
        
        
def test_modify_not_exist_bucket(bucketPolicy_controller, setup_bucket_policy):
    
    bucket_name, actions, allow_versions, bucket_policy = setup_bucket_policy
    with pytest.raises(IsNotExistFault):
        bucketPolicy_controller.modify_bucket_policy(bucket_name+'!')

def test_modify_without_parameters(bucketPolicy_controller, setup_bucket_policy):
    
    bucket_name, actions, allow_versions, bucket_policy = setup_bucket_policy
    with pytest.raises(ParamValidationFault):
        bucketPolicy_controller.modify_bucket_policy(bucket_name, action="add")
    
    
def test_get_exist_bucket_policy(bucketPolicy_controller, setup_bucket_policy):
    
    bucket_name, actions, allow_versions, bucket_policy = setup_bucket_policy
    bucketPolicy_controller.get_bucket_policy(bucket_name)
    _test_is_description_is_correct(bucket_policy, bucket_name, actions, allow_versions)
    
def test_get_not_exist_bucket_policy(bucketPolicy_controller):
    
    with pytest.raises(IsNotExistFault):
        bucketPolicy_controller.get_bucket_policy("my_bucket100")
    
    
def test_delete_exist_policy(bucketPolicy_controller, setup_bucket_policy):
    
    bucket_name, actions, allow_versions, bucket_policy = setup_bucket_policy
    bucketPolicy_controller.delete_bucket_policy(bucket_name)
    with pytest.raises(IsNotExistFault):
        bucketPolicy_controller.get_bucket_policy(bucket_name)

def test_delete_not_exist_policy(bucketPolicy_controller):
    
    with pytest.raises(IsNotExistFault):
        bucketPolicy_controller.delete_bucket_policy("my_bucket100")