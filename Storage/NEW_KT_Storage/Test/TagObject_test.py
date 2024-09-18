import pytest
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))

from Storage.NEW_KT_Storage.Service.Classes.TagObjectService import TagObjectService
from Models.TagObjectModel import TagObject


@pytest.fixture
def tag_service():
    """Create a fixture for the TagObjectService."""
    return TagObjectService()


def test_create_tag(tag_service):
    """Test that creating a TagObject works correctly."""
    key = "TestKey"
    value = "TestValue"
    result = tag_service.create(key, value)

    assert result is not None
    assert isinstance(result, TagObject)
    assert result.key == key
    assert result.value == value


def test_modify_tag(tag_service):
    """Test that modifying a TagObject works correctly."""
    key = "TestKey"
    value = "TestValue"
    tag = tag_service.create(key, value)

    # Modify the tag object
    new_value = "NewTestValue"
    tag.value = new_value
    modified_tag = tag_service.modify(tag)

    assert modified_tag is not None
    assert modified_tag.value == new_value


def test_delete_tag(tag_service):
    """Test that deleting a TagObject works correctly."""
    key = "DeleteKey"
    value = "DeleteValue"
    tag = tag_service.create(key, value)

    # Test delete functionality
    delete_result = tag_service.delete(tag)

    assert delete_result is True


def test_describe_tag(tag_service):
    """Test that describing a TagObject works correctly."""
    key = "DescribeKey"
    value = "DescribeValue"
    tag = tag_service.create(key, value)

    # Test describe functionality
    description = tag_service.describe(tag)

    assert description is not None
    assert description["key"] == key
    assert description["value"] == value
