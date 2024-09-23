import pytest
from Storage.NEW_KT_Storage.DataAccess.LifecyclePolicyManager import LifecyclePolicyManager
from Storage.NEW_KT_Storage.Models.LifecyclePolicyModel import LifecyclePolicy


@pytest.fixture
def lifecycle_manager():
    return LifecyclePolicyManager(
        object_name="TestObject",
        policy_file="test_lifecycle.json",
        db_path=":memory:",
        base_dir="./test_data"
    )


@pytest.fixture
def sample_policy():
    return LifecyclePolicy("test_policy", "bucket_name", 60, 30, "active", ["prefix1"])


def clean_up_policy(manager, policy_name):
    try:
        manager.delete(policy_name)
    except ValueError:
        pass


def test_create_delete_and_get_policy(lifecycle_manager, sample_policy):
    clean_up_policy(lifecycle_manager, sample_policy.policy_name)

    lifecycle_manager.create(sample_policy)
    retrieved_policy = lifecycle_manager.get(sample_policy.policy_name)
    assert retrieved_policy is not None

    lifecycle_manager.delete(sample_policy.policy_name)
    retrieved_policy = lifecycle_manager.get(sample_policy.policy_name)
    assert retrieved_policy is None


def test_update_non_existing_policy(lifecycle_manager, sample_policy):
    clean_up_policy(lifecycle_manager, "non_existing_policy")

    with pytest.raises(ValueError, match="Policy with name 'non_existing_policy' does not exist"):
        lifecycle_manager.update("non_existing_policy", sample_policy)


def test_create_duplicate_policy(lifecycle_manager, sample_policy):
    clean_up_policy(lifecycle_manager, sample_policy.policy_name)

    lifecycle_manager.create(sample_policy)
    with pytest.raises(ValueError, match=f"Policy with name '{sample_policy.policy_name}' already exists"):
        lifecycle_manager.create(sample_policy)


def test_create_and_get_policy(lifecycle_manager, sample_policy):
    clean_up_policy(lifecycle_manager, sample_policy.policy_name)

    lifecycle_manager.create(sample_policy)
    retrieved_policy = lifecycle_manager.get(sample_policy.policy_name)

    assert retrieved_policy.policy_name == sample_policy.policy_name
    assert retrieved_policy.expiration_days == sample_policy.expiration_days
    assert retrieved_policy.transitions_days_glacier == sample_policy.transitions_days_glacier
    assert retrieved_policy.status == sample_policy.status
    assert retrieved_policy.prefix == sample_policy.prefix


def test_create_update_and_get_policy(lifecycle_manager, sample_policy):
    clean_up_policy(lifecycle_manager, sample_policy.policy_name)

    lifecycle_manager.create(sample_policy)

    updated_policy = LifecyclePolicy(sample_policy.policy_name, "new_bucket_name", 90, 45, "inactive", ["prefix2"])
    lifecycle_manager.update(sample_policy.policy_name, updated_policy)

    retrieved_policy = lifecycle_manager.get(sample_policy.policy_name)
    assert retrieved_policy.expiration_days == updated_policy.expiration_days
    assert retrieved_policy.transitions_days_glacier == updated_policy.transitions_days_glacier
    assert retrieved_policy.status == updated_policy.status
    assert retrieved_policy.prefix == updated_policy.prefix


def test_get_non_existing_policy(lifecycle_manager):
    retrieved_policy = lifecycle_manager.get("non_existing_policy")
    assert retrieved_policy is None
