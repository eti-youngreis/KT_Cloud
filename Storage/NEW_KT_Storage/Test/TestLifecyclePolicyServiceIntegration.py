import pytest
import uuid
from Storage.NEW_KT_Storage.Service.Classes.LifecyclePolicyService import LifecyclePolicyService

@pytest.fixture
def lifecycle_service():
    return LifecyclePolicyService()

def generate_unique_name(prefix):
    return f"{prefix}_{uuid.uuid4().hex[:8]}"

def test_create_and_get_policy(lifecycle_service):
    policy_name = generate_unique_name("test_policy")
    try:
        lifecycle_service.create(policy_name, "bucket_name", 30, 20, "Enabled", ["prefix1"])
        policy = lifecycle_service.get(policy_name)
        assert policy.policy_name == policy_name
        assert policy.expiration_days == 30
        assert policy.transitions_days_glacier == 20
    finally:
        lifecycle_service.delete(policy_name)

def test_create_modify_and_get_policy(lifecycle_service):
    policy_name = generate_unique_name("test_policy")
    try:
        lifecycle_service.create(policy_name,"bucket_name", 30, 20, "Enabled", ["prefix1"])
        lifecycle_service.modify(policy_name, expiration_days=45, transitions_days_glacier=30)
        policy = lifecycle_service.get(policy_name)
        assert policy.expiration_days == 45
        assert policy.transitions_days_glacier == 30
    finally:
        lifecycle_service.delete(policy_name)

def test_create_delete_and_get_policy(lifecycle_service):
    policy_name = generate_unique_name("test_policy")
    lifecycle_service.create(policy_name,"bucket_name", 30, 20, "Enabled", ["prefix1"])
    lifecycle_service.delete(policy_name)
    policy = lifecycle_service.get(policy_name)
    assert policy is None

def test_modify_non_existing_policy(lifecycle_service):
    non_existing_policy = generate_unique_name("non_existing_policy")
    lifecycle_service.modify(non_existing_policy, expiration_days=45, transitions_days_glacier=30)
    assert lifecycle_service.get(non_existing_policy) is None

def test_delete_non_existing_policy(lifecycle_service):
    non_existing_policy = generate_unique_name("non_existing_policy")
    lifecycle_service.delete(non_existing_policy)


def test_create_policy_with_multiple_prefixes(lifecycle_service):
    policy_name = generate_unique_name("multi_prefix_policy")
    try:
        lifecycle_service.create(policy_name,"bucket_name", 30, 20, "Enabled", ["prefix1", "prefix2", "prefix3"])
        policy = lifecycle_service.get(policy_name)
        assert policy.prefix == ["prefix1", "prefix2", "prefix3"]
    finally:
        lifecycle_service.delete(policy_name)


def test_modify_policy_status(lifecycle_service):
    policy_name = generate_unique_name("status_change_policy")
    try:
        lifecycle_service.create(policy_name, "bucket_name", 30, 20, "Enabled", ["prefix1"])
        lifecycle_service.modify(policy_name, status="Disabled")
        policy = lifecycle_service.get(policy_name)
        assert policy.status == "Disabled"
    finally:
        lifecycle_service.delete(policy_name)


def test_create_policy_with_max_values(lifecycle_service):
    policy_name = generate_unique_name("max_value_policy")
    try:
        lifecycle_service.create(policy_name, "bucket_name", 3650, 3650, "Enabled", ["prefix1"])
        policy = lifecycle_service.get(policy_name)
        assert policy.expiration_days == 3650
        assert policy.transitions_days_glacier == 3650
    finally:
        lifecycle_service.delete(policy_name)


def test_modify_multiple_attributes(lifecycle_service):
    policy_name = generate_unique_name("multi_modify_policy")
    try:
        lifecycle_service.create(policy_name, "bucket_name", 30, 20, "Enabled", ["prefix1"])
        lifecycle_service.modify(policy_name, expiration_days=60, transitions_days_glacier=40, status="Disabled",
                                 prefix=["new_prefix"])
        policy = lifecycle_service.get(policy_name)
        assert policy.expiration_days == 60
        assert policy.transitions_days_glacier == 40
        assert policy.status == "Disabled"
        assert policy.prefix == ["new_prefix"]
    finally:
        lifecycle_service.delete(policy_name)


def test_create_multiple_policies(lifecycle_service):
    policy_names = [generate_unique_name("multi_policy") for _ in range(3)]
    try:
        for i, name in enumerate(policy_names):
            lifecycle_service.create(name, "bucket_name", 30 + i, 20 + i, "Enabled", [f"prefix{i}"])

        for i, name in enumerate(policy_names):
            policy = lifecycle_service.get(name)
            assert policy.policy_name == name
            assert policy.expiration_days == 30 + i
            assert policy.transitions_days_glacier == 20 + i
            assert policy.prefix == [f"prefix{i}"]
    finally:
        for name in policy_names:
            lifecycle_service.delete(name)