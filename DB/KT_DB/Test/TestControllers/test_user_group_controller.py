import os
import sys
import pytest
import sqlite3
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from DB_UserAdministration.Controllers.DBUserGroupController import UserGroupController,UserGroupService
from DB_UserAdministration.DataAccess.UserGroupManager import UserGroupManager

@pytest.fixture
def user_group_manager():
    return UserGroupManager(':memory:')

@pytest.fixture
def user_group_service(user_group_manager):
    return UserGroupService(user_group_manager)

@pytest.fixture
def user_group_controller(user_group_service):
    return UserGroupController(user_group_service)


def test_create_group(user_group_controller):
    group_name = "TestGroup"
    result = user_group_controller.create_group(group_name)
    assert result['Group']['name'] == group_name

def test_create_group_invalid_name(user_group_controller):
    with pytest.raises(ValueError):
        user_group_controller.create_group("Invalid Group Name!")

def test_delete_group(user_group_controller):
    group_name = "TestGroupToDelete"
    user_group_controller.create_group(group_name)
    user_group_controller.delete_group(group_name)
    with pytest.raises(FileNotFoundError):
        user_group_controller.get_group(group_name)

def test_update_group(user_group_controller):
    group_name = "TestGroup"
    new_group_name = "UpdatedGroupName"
    user_group_controller.create_group(group_name)
    user_group_controller.update_group(group_name, new_group_name)
    result = user_group_controller.get_group(new_group_name)
    assert result['Group']['name'] == new_group_name

def test_get_group(user_group_controller):
    group_name = "TestGroup"
    user_group_controller.create_group(group_name)
    result = user_group_controller.get_group(group_name)
    assert result['Group']['name'] == group_name

def test_list_groups(user_group_controller):
    group_name1 = "TestGroup1"
    group_name2 = "TestGroup2"
    user_group_controller.create_group(group_name1)
    user_group_controller.create_group(group_name2)
    result = user_group_controller.list_groups()
    assert len(result) == 2

def test_add_member_to_group(user_group_controller):
    group_name = "TestGroup"
    user_id = "user1"
    user_group_controller.create_group(group_name)
    user_group_controller.add_member_to_group(group_name, user_id)
    result = user_group_controller.get_group(group_name)
    assert user_id in result['Group']['users']

def test_remove_member_from_group(user_group_controller):
    group_name = "TestGroup"
    user_id = "user1"
    user_group_controller.create_group(group_name)
    user_group_controller.add_member_to_group(group_name, user_id)
    user_group_controller.remove_member_from_group(group_name, user_id)
    result = user_group_controller.get_group(group_name)
    assert user_id not in result['Group']['users']

def test_assign_permission_to_group(user_group_controller):
    group_name = "TestGroup"
    policy = "policy1"
    user_group_controller.create_group(group_name)
    user_group_controller.assign_permission_to_group(group_name, policy)
    result = user_group_controller.get_group(group_name)
    assert policy in result['Group']['policies']

def test_revoke_permission_from_group(user_group_controller):
    group_name = "TestGroup"
    policy = "policy1"
    user_group_controller.create_group(group_name)
    user_group_controller.assign_permission_to_group(group_name, policy)
    user_group_controller.revoke_permission_from_group(group_name, policy)
    result = user_group_controller.get_group(group_name)
    assert policy not in result['Group']['policies']
