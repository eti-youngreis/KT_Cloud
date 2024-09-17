import pytest
from Controller.BucketPolicyController import BucketPolicyController
from DataAccess.BucketPolicyManager import BucketPolicyManager
from Models.BucketPolicyModel import BucketPolicy
from Service.Classes.BucketPolicyService import BucketPolicyService

@pytest.fixture
def bucketPolicy_manager():
    return BucketPolicyManager()

@pytest.fixture
def bucketPolicy_service(bucketPolicy_manager:BucketPolicyManager):
    return BucketPolicyService(bucketPolicy_manager)

@pytest.fixture
def bucketPolicy_controller(bucketPolicy_service:BucketPolicyService):
    return BucketPolicyController(bucketPolicy_service)