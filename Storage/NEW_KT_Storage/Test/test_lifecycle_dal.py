import pytest
import json
from unittest.mock import Mock, patch, mock_open
from Storage.NEW_KT_Storage.Models.LifecyclePolicyModel import LifecyclePolicy
from Storage.NEW_KT_Storage.DataAccess.LifecyclePolicyManager import LifecyclePolicyManager as LifecycleDal

@pytest.fixture
def lifecycle_dal():
    # Mock StorageManager and ObjectManager
    with patch(
            'Storage.NEW_KT_Storage.DataAccess.LifecyclePolicyManager.StorageManager') as MockStorageManager, \
            patch(
                'Storage.NEW_KT_Storage.DataAccess.LifecyclePolicyManager.ObjectManager') as MockObjectManager:
        # Create the LifecycleDal instance
        lifecycle_dal = LifecycleDal(
            path_physical_object="test_lifecycle.json",
            path_db="C:\\Users\\User\\Desktop\\test_database\\test_Lifecycle.db",
            base_directory="C:\\Users\\User\\Desktop\\test_server"
        )

        # Replace the actual StorageManager and ObjectManager with mocks
        lifecycle_dal.storage_manager = MockStorageManager.return_value
        lifecycle_dal.object_manager = MockObjectManager.return_value

        # Mock methods that might be called during tests
        lifecycle_dal.storage_manager.is_file_exist.return_value = False
        lifecycle_dal.object_manager.save_in_memory.return_value = None

        return lifecycle_dal


@pytest.fixture
def lifecycle_policy():
    return LifecyclePolicy(policy_name="test_policy", expiration_days=4, transitions_days_GLACIER=5)


def test_create_new_file(lifecycle_dal, lifecycle_policy):
    lifecycle_dal.storage_manager.is_file_exist.return_value = False
    mock_file = mock_open()

    with patch('builtins.open', mock_file):
        lifecycle_dal.create(lifecycle_policy)

    lifecycle_dal.storage_manager.is_file_exist.assert_called_once_with(lifecycle_dal.json_name)
    mock_file.assert_called_once_with(lifecycle_dal.path_physical_object, 'w')
    handle = mock_file()

    # Use default_converter for JSON serialization
    json_data = json.dumps({lifecycle_policy.policy_name: lifecycle_policy.__dict__},
                           indent=4, ensure_ascii=False, default=LifecycleDal.default_converter)

    # Collect all written data
    written_data = ''.join(call.args[0] for call in handle.write.call_args_list)

    # Compare written data with expected JSON data
    assert written_data == json_data

    lifecycle_dal.object_manager.save_in_memory.assert_called_once_with(lifecycle_dal.object_name,
                                                                        lifecycle_policy.to_sql())


def test_create_existing_file(lifecycle_dal, lifecycle_policy):
    lifecycle_dal.storage_manager.is_file_exist.return_value = True
    existing_data = {"existing_policy": {"some": "data"}}
    mock_file = mock_open(read_data=json.dumps(existing_data))

    with patch('builtins.open', mock_file):
        lifecycle_dal.create(lifecycle_policy)

    lifecycle_dal.storage_manager.is_file_exist.assert_called_once_with(lifecycle_dal.json_name)
    assert mock_file.call_count == 2  # Once for read, once for write
    handle = mock_file()
    expected_data = existing_data.copy()
    expected_data[lifecycle_policy.policy_name] = lifecycle_policy.__dict__
    json_data = json.dumps(expected_data, indent=4, ensure_ascii=False, default=LifecycleDal.default_converter)

    # Instead of asserting a single write, we'll check if all the expected content was written
    written_data = ''.join(call.args[0] for call in handle.write.call_args_list)
    assert written_data == json_data

    lifecycle_dal.object_manager.save_in_memory.assert_called_once_with(lifecycle_dal.object_name,
                                                                        lifecycle_policy.to_sql())


def test_create_file_write_error(lifecycle_dal, lifecycle_policy):
    lifecycle_dal.storage_manager.is_file_exist.return_value = False
    mock_file = mock_open()
    mock_file.side_effect = IOError("Unable to write file")

    with patch('builtins.open', mock_file):
        with pytest.raises(IOError):
            lifecycle_dal.create(lifecycle_policy)

    lifecycle_dal.storage_manager.is_file_exist.assert_called_once_with(lifecycle_dal.json_name)
    lifecycle_dal.object_manager.save_in_memory.assert_not_called()


def test_create_db_save_error(lifecycle_dal, lifecycle_policy):
    lifecycle_dal.storage_manager.is_file_exist.return_value = False
    lifecycle_dal.object_manager.save_in_memory.side_effect = Exception("DB save error")
    mock_file = mock_open()

    with patch('builtins.open', mock_file):
        with pytest.raises(Exception, match="DB save error"):
            lifecycle_dal.create(lifecycle_policy)

    lifecycle_dal.storage_manager.is_file_exist.assert_called_once_with(lifecycle_dal.json_name)
    mock_file.assert_called_once_with(lifecycle_dal.path_physical_object, 'w')
    lifecycle_dal.object_manager.save_in_memory.assert_called_once_with(lifecycle_dal.object_name,
                                                                        lifecycle_policy.to_sql())
