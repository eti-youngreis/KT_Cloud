import pytest
from unittest.mock import MagicMock
from Service.Classes.OptionGroupService import OptionGroupService
from DataAccess.DataAccessLayer import DataAccessLayer
from Models.OptionGroupModel import OptionGroupModel

@pytest.fixture
def mock_dal():
    """Fixture to create a mock DataAccessLayer."""
    return MagicMock(spec=DataAccessLayer)

@pytest.fixture
def option_group_service(mock_dal):
    """Fixture to create an OptionGroupService instance with a mocked DataAccessLayer."""
    return OptionGroupService(dal=mock_dal)

def test_create_option_group(option_group_service, mock_dal):
    """Test creating an Option Group."""
    engine_name = "test-engine"
    major_engine_version = "1.0"
    option_group_description = "Test Description"
    option_group_name = "test-option-group"
    tags = {"key1": "value1"}

    # Mock validation functions
    with pytest.raises(ValueError, match="Invalid engineName"):
        option_group_service.create("", major_engine_version, option_group_description, option_group_name, tags)

    with pytest.raises(ValueError, match="Invalid optionGroupName"):
        option_group_service.create(engine_name, major_engine_version, option_group_description, "", tags)

    # Mock successful creation
    mock_dal.insert.return_value = None
    option_group_service.create(engine_name, major_engine_version, option_group_description, option_group_name, tags)
    mock_dal.insert.assert_called_once_with('OptionGroup', {
        'engineName': engine_name,
        'majorEngineVersion': major_engine_version,
        'optionGroupDescription': option_group_description,
        'optionGroupName': option_group_name,
        'tags': tags
    })
