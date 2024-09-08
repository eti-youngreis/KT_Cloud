from DB_UserAdministration.Services.PolicyService import PolicyService
from DB_UserAdministration.DataAccess.PolicyManager import PolicyManager
import pytest
import os
import sys

project_root = os.path.abspath(os.path.join(
    os.path.dirname(__file__), '..', '..', '..'))
sys.path.insert(0, project_root)


@pytest.fixture
def policy_service():
    policy_manager = PolicyManager(
        'DB_UserAdministration/tests/policy_tests/test.db')
    service = PolicyService(policy_manager)
    yield service
    service.delete('test_policy')


def test_policy_service_create(policy_service):
    policy_service.create('test_policy')
    assert policy_service.get('test_policy')


def test_policy_service_delete(policy_service):
    policy_service.create('test_policy')
    policy_service.delete('test_policy')
    assert policy_service.policy_manager.is_identifier_exist(
        'test_policy') == False