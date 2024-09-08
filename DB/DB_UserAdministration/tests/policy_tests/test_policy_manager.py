import os
import sys

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
sys.path.insert(0, project_root)

import pytest

from DB_UserAdministration.DataAccess.PolicyManager import PolicyManager
from DB_UserAdministration.Models.PolicyModel import Policy
from DB_UserAdministration.Models.permissionModel import Effect, Permission, Action, Resource
@pytest.fixture
def policy_manager():
    manager = PolicyManager('DB_UserAdministration/tests/policy_tests/test.db')
    yield manager
    manager.delete('test_policy')

class TestPolicyManager:

    def test_is_json_column_contains_key_and_value_with_contained(self, policy_manager):
        policy_test = Policy('test_policy')
        policy_manager.create(policy_test.to_dict())
        assert policy_manager.is_json_column_contains_key_and_value('policy_id', policy_test.name)

    def test_is_json_column_contains_key_and_value_with_not_contained(self, policy_manager):
        assert not policy_manager.is_json_column_contains_key_and_value('id', 'test_policy')

    def test_is_identifier_exist_with_existing(self, policy_manager):
        policy_manager.create(Policy('test_policy').to_dict())
        assert policy_manager.is_identifier_exist('test_policy')

    def test_is_identifier_exist_with_not_existing(self, policy_manager):
        assert not policy_manager.is_identifier_exist('test_policy')

    def test_get(self, policy_manager):
        policy_test = Policy('test_policy')
        policy_manager.create(policy_test.to_dict())
        assert policy_manager.get(policy_test.name) == policy_test.to_dict()

    def test_create(self, policy_manager):
        policy_test = Policy('test_policy')
        policy_manager.create(metadata=policy_test.to_dict())
        assert policy_manager.get(policy_test.name)

    def test_delete(self, policy_manager):
        policy_test = Policy('test_policy')
        policy_manager.create(policy_test.to_dict())
        policy_manager.delete(policy_test.name)
        assert not policy_manager.is_identifier_exist('test_policy')

    def test_update(self, policy_manager):
        policy_test = Policy('test_policy')
        policy_manager.create(policy_test.to_dict())
        permission_id = Permission.get_id_by_permission(Action.READ, Resource.DATABASE, Effect.ALLOW)
        policy_test.permissions.append(permission_id)     
        policy_manager.update(policy_test.name, policy_test.to_dict())
        assert policy_manager.get(policy_test.name) == policy_test.to_dict()

    # def test_load(self):
    #     print(self.policy_controller.list_policies())

    # def test_add_permission(self):
    #     try:
    #         self.policy_controller.create_policy('test_policy')
    #         permission = Permission(Action.READ, Resource.DATABASE, Effect.ALLOW)
    #         self.policy_controller.add_permission('test_policy', permission)
    #         assert self.policy_controller.get_policy('test_policy')
    #     finally:
    #         self.policy_controller.delete_policy('test_policy')