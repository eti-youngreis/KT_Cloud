import os
import sys

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
sys.path.insert(0, project_root)

import pytest
from DB_UserAdministration.Exceptions.PolicyException import DuplicatePolicyError
from DB_UserAdministration.DataAccess.PolicyManager import PolicyManager
from DB_UserAdministration.Services.PolicyService import PolicyService
from DB_UserAdministration.Controllers.PolicyController import PolicyController

@pytest.fixture
def policy_controller():
    policy_manager = PolicyManager('DB_UserAdministration/tests/policy_tests/test.db')
    policy_service = PolicyService(policy_manager)
    controller = PolicyController(policy_service)
    yield controller
    controller.delete_policy('test_policy')

def test_create(policy_controller):
    policy_controller.create_policy('test_policy')
    assert policy_controller.get_policy('test_policy')

def test_create_with_existing_name(policy_controller):
    policy_controller.create_policy('test_policy')
    with pytest.raises(DuplicatePolicyError):
        policy_controller.create_policy('test_policy')

def test_create_without_name(policy_controller):
    with pytest.raises(TypeError):
        policy_controller.create_policy()

def test_delete(policy_controller):
    policy_controller.create_policy('test_policy')
    policy_controller.delete_policy('test_policy')
    assert policy_controller.service.policy_manager.is_identifier_exist('test_policy') == False

def test_delete_with_nonexistent_policy(policy_controller):
    policy_controller.delete_policy('test_policy')