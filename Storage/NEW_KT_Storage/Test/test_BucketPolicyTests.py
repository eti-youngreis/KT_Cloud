import tempfile
import pytest
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))
from Storage.NEW_KT_Storage.Controller.BucketPolicyController import BucketPolicyController
from Storage.NEW_KT_Storage.Service.Classes.BucketPolicyService import BucketPolicyService, ParamValidationFault, IsExistPermissionFault, IsNotExistFault, IsNotExistPermissionFault 
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
    with tempfile.TemporaryDirectory() as tmpdir:
        bucketPolicy_manager.storage_manager = StorageManager(tmpdir)
        
    return bucketPolicy_manager

@pytest.fixture
def bucketPolicy_service(bucketPolicy_manager:BucketPolicyManager):
    return BucketPolicyService(bucketPolicy_manager)

@pytest.fixture
def bucketPolicy_controller(bucketPolicy_service:BucketPolicyService):
    return BucketPolicyController(bucketPolicy_service)

@pytest.fixture
def setup_bucket_policy(bucketPolicy_controller:BucketPolicyController):
    bucket_name = "my_bucket"
    permissions = []
    allow_versions = True
    
    bucket_policy = bucketPolicy_controller.create_bucket_policy(bucket_name, permissions, allow_versions)
    return bucket_name, permissions, allow_versions, bucket_policy

def _test_is_description_is_correct(description, bucket_name, permissions, allow_versions):
    
    assert description['bucket_name'] == bucket_name
    assert description['permissions'] == permissions
    assert description['allow_versions'] == allow_versions

def test_create_bucket_policy_success(setup_bucket_policy):
    
    bucket_name, permissions, allow_versions, bucket_policy = setup_bucket_policy
    _test_is_description_is_correct(bucket_policy, bucket_name, permissions, allow_versions)
    
"delete"
def test_create_invalid_bucket_name(bucketPolicy_controller):
    
    with pytest.raises(ParamValidationFault):
        bucketPolicy_controller.create_bucket_policy()
        
def test_create_inavlid_permission(bucketPolicy_controller):
    
    with pytest.raises(ParamValidationFault):
        bucketPolicy_controller.create_bucket_policy(bucket_name="my_bucket", permissions="yyy")
        
def test_create_invalud_allow_versions(bucketPolicy_controller):
    
    with pytest.raises(ParamValidationFault):
        bucketPolicy_controller.create_bucket_policy(bucket_name="my_bucket", allow_versions="yes")
        
def test_add_permission_to_bucket_policy(bucketPolicy_controller, setup_bucket_policy):
    
    bucket_name, permissions, allow_versions, bucket_policy = setup_bucket_policy
    bucketPolicy_controller.modify_bucket_policy(bucket_name, update_permmisions=['READ'], action="add")
    
def test_delete_permission_from_bucket_policy(bucketPolicy_controller, setup_bucket_policy):
    
    bucketPolicy_controller.create_bucket_policy("my_bucket", ['READ'])
    bucketPolicy_controller.modify_bucket_policy("my_bucket", action="delete", update_permmisions=['READ'])
    
def test_add_exist_permission(bucketPolicy_controller):
    
    bucketPolicy_controller.create_bucket_policy("my_bucket", ['READ'])
    with pytest.raises(IsExistPermissionFault):
        bucketPolicy_controller.modify_bucket_policy("my_bucket", action="add", update_permmisions=['READ']) 

def test_delete_not_exist_permission(bucketPolicy_controller):
    
    bucketPolicy_controller.create_bucket_policy("my_bucket", ['READ'])
    with pytest.raises(IsNotExistPermissionFault):
        bucketPolicy_controller.modify_bucket_policy("my_bucket", action="delete", update_permmisions=['WRITE'])
    
# def test_modify_bucket_policy(bucketPolicy_controller, setup_bucket_policy):
    
#     bucket_name, permissions, allow_versions, bucket_policy = setup_bucket_policy
#     bucketPolicy_controller.modify_bucket_policy(bucket_name, update_permmisions=['WRITE'])

def test_modify_when_allow_versions_invalid(bucketPolicy_controller, setup_bucket_policy):
    
    bucket_name, permissions, allow_versions, bucket_policy = setup_bucket_policy
    with pytest.raises(ParamValidationFault):
        bucketPolicy_controller.modify_bucket_policy(bucket_name, allow_versions="")
    

# def test_modify_when_the_permission_exist(bucketPolicy_controller, setup_bucket_policy):
#     bucket_name, permissions, allow_versions, bucket_policy = setup_bucket_policy
#     bucketPolicy_controller.modify_bucket_policy(bucket_name, ['READ'])
#     with pytest.raises(Exception):
#         bucketPolicy_controller.modify_bucket_policy(bucket_name, ['READ'])
    
# def test_modify_invalid_permission(bucketPolicy_controller, setup_bucket_policy):
    
#     bucket_name, permissions, allow_versions, bucket_policy = setup_bucket_policy
#     with pytest.raises(ParamValidationFault):
#         bucketPolicy_controller.modify_bucket_policy(bucket_name, action="add", update_permmisions="READ")
        
def test_modify_noe_exist_bucket(bucketPolicy_controller, setup_bucket_policy):
    
    bucket_name, permissions, allow_versions, bucket_policy = setup_bucket_policy
    with pytest.raises(IsNotExistFault):
        bucketPolicy_controller.modify_bucket_policy(bucket_name+'!')

# def test_modify_without_parameters(bucketPolicy_controller, setup_bucket_policy):
    
    
def test_get_exist_bucket_policy(bucketPolicy_controller, setup_bucket_policy):
    
    bucket_name, permissions, allow_versions, bucket_policy = setup_bucket_policy
    bucketPolicy_controller.get_bucket_policy(bucket_name)
    
def test_get_not_exist_bucket_policy(bucketPolicy_controller):
    
    with pytest.raises(IsNotExistFault):
        bucketPolicy_controller.get_bucket_policy("my_bucket100")
    
    
def test_delete_exist_policy(bucketPolicy_controller, setup_bucket_policy):
    bucket_name, permissions, allow_versions, bucket_policy = setup_bucket_policy
    bucketPolicy_controller.delete_bucket_policy(bucket_name)

def test_delete_not_exist_policy(bucketPolicy_controller):
    with pytest.raises(IsNotExistFault):
        bucketPolicy_controller.delete_bucket_policy("my_bucket100")