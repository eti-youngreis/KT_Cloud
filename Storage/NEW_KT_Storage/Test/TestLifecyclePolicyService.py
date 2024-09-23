import pytest
from unittest.mock import Mock
from Storage.NEW_KT_Storage.Service.Classes.LifecyclePolicyService import LifecyclePolicyService
from Storage.NEW_KT_Storage.Models.LifecyclePolicyModel import LifecyclePolicy


@pytest.fixture
def lifecycle_service():
    service = LifecyclePolicyService()
    service.manager = Mock()
    return service

@pytest.fixture(autouse=True)
def cleanup(lifecycle_service):
    yield
    # This code runs after each test
    lifecycle_service.delete("test_policy")

def test_create_lifecycle_policy(lifecycle_service):
    lifecycle_service.get = Mock(return_value=None)

    lifecycle_service.create("test_policy", "bucket_name", 30, 60, "Enabled", ["prefix1"])

    lifecycle_service.get.assert_called_once_with("test_policy")
    lifecycle_service.manager.create.assert_called_once()
    created_policy = lifecycle_service.manager.create.call_args[0][0]
    assert isinstance(created_policy, LifecyclePolicy)
    assert created_policy.policy_name == "test_policy"
    assert created_policy.expiration_days == 30
    assert created_policy.transitions_days_glacier == 60
    assert created_policy.status == "Enabled"
    assert created_policy.prefix == ["prefix1"]


def test_get_existing_policy(lifecycle_service):
    expected_policy = LifecyclePolicy("test_policy", "bucket_name", 30, 60, "active", ["prefix1"])
    lifecycle_service.manager.get.return_value = expected_policy
    result = lifecycle_service.get("test_policy")
    assert result == expected_policy


def test_delete_existing_policy(lifecycle_service):
    lifecycle_service.delete("test_policy")
    lifecycle_service.manager.delete.assert_called_once_with(policy_name="test_policy")


def test_modify_existing_policy(lifecycle_service):
    original_policy = LifecyclePolicy("test_policy", "bucket_name", 30, 60, "Enabled", ["prefix1"])
    lifecycle_service.get = Mock(return_value=original_policy)

    lifecycle_service.modify("test_policy", expiration_days=70)

    lifecycle_service.manager.update.assert_called_once()
    assert original_policy.expiration_days == 70


def test_create_duplicate_policy(lifecycle_service):
    existing_policy = LifecyclePolicy("test_policy","bucket_name", 30, 60, "Enabled", ["prefix1"])
    lifecycle_service.get = Mock(return_value=existing_policy)

    with pytest.raises(ValueError, match="Policy with name 'test_policy' already exists"):
        lifecycle_service.create("test_policy", "bucket_name", 30, 60, "Enabled", ["prefix1"])


def test_get_non_existing_policy(lifecycle_service):
    lifecycle_service.manager.get.return_value = None
    result = lifecycle_service.get("non_existing_policy")
    assert result is None


def test_modify_non_existing_policy(lifecycle_service, capfd):
    lifecycle_service.get = Mock(return_value=None)
    lifecycle_service.modify("non_existing_policy", "bucket_name", expiration_days=45)
    captured = capfd.readouterr()
    assert "Policy 'non_existing_policy' not found" in captured.out


def test_create_policy_with_invalid_expiration_days(lifecycle_service):
    lifecycle_service.delete("test_policy")
    with pytest.raises(ValueError):
        lifecycle_service.create("test_policy", "bucket_name", -1, 60, "active", ["prefix1"])














