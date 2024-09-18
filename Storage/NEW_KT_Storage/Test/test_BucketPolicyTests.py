import pytest
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))
from Storage.NEW_KT_Storage.Controller.BucketPolicyController import BucketPolicyController
from Storage.NEW_KT_Storage.Service.Classes.BucketPolicyService import BucketPolicyService, ParamValidationFault, IsExistPermissionFault, IsNotExistFault 
from Storage.NEW_KT_Storage.DataAccess.BucketPolicyManager import BucketPolicyManager
from Storage.NEW_KT_Storage.DataAccess.StorageManager import StorageManager
from Storage.NEW_KT_Storage.DataAccess.ObjectManager import ObjectManager
from Storage.NEW_KT_Storage.Models.BucketPolicyModel import BucketPolicy

@pytest.fixture
def storage_manager():
    return StorageManager()
@pytest.fixture
def bucketPolicy_manager():
    return BucketPolicyManager()

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
    
def test_create_without_name(bucketPolicy_controller:BucketPolicyController):
    
    with pytest.raises(ParamValidationFault):
        bucketPolicy_controller.create_bucket_policy()

    
def test_modify_when_the_permission_exist(bucketPolicy_controller, setup_bucket_policy):
    bucket_name, permissions, allow_versions, bucket_policy = setup_bucket_policy
    bucketPolicy_controller.modify_bucket_policy(bucket_name, "read")
    bucketPolicy_controller.modify_bucket_policy(bucket_name, "read")
    
# def test_delete_exist_policy()
# def test_delete_not_exist_policy()