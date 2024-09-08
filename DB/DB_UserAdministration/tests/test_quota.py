import pytest
from DB_UserAdministration.Services.QuotaService import QuotaService
from DB_UserAdministration.Controllers.QuotaController import QuotaController
from DB_UserAdministration.DataAccess.QuotaManager import QuotaManager

@pytest.fixture(scope='module')
def quota_manager():
    """Fixture to create a temporary QuotaManager."""
    db_file = ':memory:'  # Using in-memory database for testing
    return QuotaManager(db_file)

@pytest.fixture(scope='module')
def quota_service(quota_manager):
    """Fixture to create a QuotaService using the QuotaManager."""
    service = QuotaService(quota_manager)
    return service

@pytest.fixture(scope='module')
def quota_controller(quota_service):
    """Fixture to create a QuotaController using the QuotaService."""
    controller = QuotaController(quota_service)
    return controller

@pytest.fixture(scope='module')
def owner_id():
    """Fixture to provide a default owner_id for testing."""
    return "test_owner"

@pytest.fixture(scope='module')
def create_default_quotas(quota_service, owner_id):
    """Fixture to create default quotas for the owner_id."""
    quota_service.create_default_quotas(owner_id)



def test_get_quota(quota_controller, owner_id, create_default_quotas):
    """
    Test the get_quota function to ensure it returns the correct quota details.
    """
    resource_type = 'DB_INSTANCE'
    quota = quota_controller.get_quota(owner_id, resource_type)
    assert isinstance(quota, dict), "Expected a dictionary for quota details"
    quota_id = f"{owner_id}_{resource_type}"
    assert quota_id in quota, "Quota ID not found in response"
    assert quota[quota_id]["resource_type"] == resource_type, "Quota resource type does not match"

def test_update_usage(quota_controller, owner_id, create_default_quotas):
    """
    Test the update_usage function to ensure it correctly updates quota usage.
    """
    resource_type = 'DB_INSTANCE'
    quota_controller.update_usage(owner_id, resource_type, amount=10)
    quota = quota_controller.get_quota(owner_id, resource_type)
    quota_id = f"{owner_id}_{resource_type}"
    assert quota[quota_id]["usage"] == 10, "Quota usage not updated correctly"

def test_check_exceeded(quota_controller, owner_id, create_default_quotas):
    """
    Test the check_exceeded function to ensure it correctly identifies when a quota is exceeded.
    """
    resource_type = 'DB_INSTANCE'
    quota_controller.update_usage(owner_id, resource_type, amount=40)  # Assuming default limit is 40
    exceeded = quota_controller.check_exceeded(owner_id, resource_type)
    assert exceeded is True, "Expected quota to be exceeded"

    # Reset the usage for the next test
    quota_controller.reset_usage(owner_id, resource_type)
    exceeded = quota_controller.check_exceeded(owner_id, resource_type)
    assert exceeded is False, "Expected quota to not be exceeded after reset"
