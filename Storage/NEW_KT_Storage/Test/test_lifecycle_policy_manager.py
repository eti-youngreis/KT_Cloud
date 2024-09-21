import pytest
from unittest.mock import Mock
from Storage.NEW_KT_Storage.DataAccess.LifecyclePolicyManager import LifecyclePolicyManager
from Storage.NEW_KT_Storage.Models.LifecyclePolicyModel import LifecyclePolicy


@pytest.fixture
def lifecycle_manager():
    return LifecyclePolicyManager("TestObject")

@pytest.fixture(autouse=True)
def cleanup(lifecycle_manager):
    yield
    # This code runs after each test
    lifecycle_manager.delete("test_policy")

# Happy path unit tests
def test_create_lifecycle_policy(lifecycle_manager):
    policy = LifecyclePolicy("test_policy", 30, 60, "active", ["prefix1"])
    lifecycle_manager.storage_manager.write_json_file = Mock()
    lifecycle_manager.object_manager.save_in_memory = Mock()

    lifecycle_manager.create(policy)

    lifecycle_manager.storage_manager.write_json_file.assert_called_once()
    lifecycle_manager.object_manager.save_in_memory.assert_called_once()


def test_get_existing_policy(lifecycle_manager):
    expected_policy = LifecyclePolicy("test_policy", 30, 60, "active", ["prefix1"])
    lifecycle_manager._read_json = Mock(return_value={"test_policy": expected_policy.__dict__})

    result = lifecycle_manager.get("test_policy")

    assert result.policy_name == expected_policy.policy_name
    assert result.expiration_days == expected_policy.expiration_days


def test_update_existing_policy(lifecycle_manager):
    original_policy = LifecyclePolicy("test_policy", 30, 60, "active", ["prefix1"])
    updated_policy = LifecyclePolicy("test_policy", 45, 90, "inactive", ["prefix2"])

    lifecycle_manager._read_json = Mock(return_value={"test_policy": original_policy.__dict__})
    lifecycle_manager._write_json = Mock()
    lifecycle_manager.object_manager.update_in_memory = Mock()

    lifecycle_manager.update("test_policy", updated_policy)

    lifecycle_manager._write_json.assert_called_once()
    lifecycle_manager.object_manager.update_in_memory.assert_called_once()


# Unhappy path unit tests
def test_get_non_existing_policy(lifecycle_manager):
    lifecycle_manager._read_json = Mock(return_value={})

    result = lifecycle_manager.get("non_existing_policy")

    assert result is None


def test_delete_non_existing_policy(lifecycle_manager):
    lifecycle_manager._read_json = Mock(return_value={})
    lifecycle_manager._write_json = Mock()
    lifecycle_manager.object_manager.delete_from_memory_by_pk = Mock()

    lifecycle_manager.delete("non_existing_policy")

    lifecycle_manager._write_json.assert_not_called()
    lifecycle_manager.object_manager.delete_from_memory_by_pk.assert_called_once()


def test_describe_non_existing_policy(lifecycle_manager):
    lifecycle_manager._read_json = Mock(return_value={})

    with pytest.raises(KeyError):
        lifecycle_manager.describe("non_existing_policy")
