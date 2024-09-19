import pytest
import os
import sys

sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
)

from Storage.NEW_KT_Storage.Service.Classes.TagObjectService import TagObjectService
from Models.TagObjectModel import TagObject


@pytest.fixture(scope="function")
def tag_service():
    tag_service = TagObjectService(db_file=":memory:")
    return tag_service


def test_create_tag(tag_service):
    """Test the creation of a new tag."""
    key = "test_key"
    value = "test_value"
    tag_service.create(key, value)

    # Assert that the tag is created and exists in memory
    tag = tag_service.get(key)
    assert tag is not None
    assert tag.key == key
    assert tag.value == value


def test_get_tag(tag_service):
    """Test retrieving a tag by key."""
    key = "get_test_key"
    value = "get_test_value"
    tag_service.create(key, value)

    # Retrieve the tag and verify its correctness
    tag = tag_service.get(key)
    assert tag is not None
    assert tag.key == key
    assert tag.value == value


def test_raise_KeyError_where_get_not_exist_tag(tag_service):
    key_that_not_exist = "key_that_not_exist"
    with pytest.raises(KeyError):
        tag_service.get(key_that_not_exist)


def test_modify_tag(tag_service):
    """Test modifying an existing tag."""
    key = "modify_test_key"
    value = "modify_test_value"
    new_key = "new_test_key"
    new_value = "new_test_value"

    # Create a tag and modify it
    tag_service.create(key, value)
    tag_service.modify(old_key=key, key=new_key, value=new_value)

    # Verify the tag was updated
    modified_tag = tag_service.get(new_key)
    assert modified_tag is not None
    assert modified_tag.key == new_key
    assert modified_tag.value == new_value


def test_modify_tag_key_to_existing_key(tag_service):
    """Test modifying a tag's key to an already existing key."""
    key1 = "existing_key_1"
    value1 = "value1"
    key2 = "existing_key_2"
    value2 = "value2"
    tag_service.create(key1, value1)
    tag_service.create(key2, value2)

    # Attempt to modify key1 to key2, which already exists
    with pytest.raises(Exception):  # Adjust based on how your system handles this
        tag_service.modify(old_key=key1, key=key2)


def test_get_after_modify_key(tag_service):
    """Test that the original key is no longer accessible after modifying the key."""
    key = "mod_key_test"
    value = "mod_value_test"
    new_key = "new_mod_key"
    tag_service.create(key, value)

    # Modify the key
    tag_service.modify(old_key=key, key=new_key)

    # The old key should raise a KeyError
    with pytest.raises(KeyError):
        tag_service.get(key)


def test_delete_tag(tag_service):
    """Test deleting an existing tag."""
    key = "delete_test_key"
    value = "delete_test_value"
    tag_service.create(key, value)

    # Delete the tag and verify it's gone
    tag_service.delete(key)

    with pytest.raises(KeyError):
        tag_service.get(key)


def test_delete_non_existent_tag(tag_service):
    """Test attempting to delete a non-existent tag."""
    non_existent_key = "non_existent_key"

    # Attempting to delete should not raise an error but should have no effect
    with pytest.raises(KeyError):
        tag_service.delete(non_existent_key)


def test_modify_non_existent_tag(tag_service):
    """Test modifying a tag that does not exist."""
    non_existent_key = "non_existent_key"

    # Modifying a non-existent tag should raise an error
    with pytest.raises(KeyError):
        tag_service.modify(old_key=non_existent_key, key="new_key", value="new_value")


def test_create_duplicate_tag(tag_service):
    """Test creating a tag with a duplicate key."""
    key = "duplicate_key"
    value1 = "value1"
    value2 = "value2"

    # Create the first tag
    tag_service.create(key, value1)

    # Creating a second tag with the same key should raise an error or overwrite
    with pytest.raises(
        Exception
    ):  # Modify this according to how duplicates are handled
        tag_service.create(key, value2)


def test_create_with_empty_key(tag_service):
    """Test creating a tag with an empty key."""
    with pytest.raises(ValueError):  # Adjust based on the actual exception
        tag_service.create("", "value_with_empty_key")


def test_create_with_empty_value(tag_service):
    """Test creating a tag with an empty value."""
    key = "empty_value_key"
    tag_service.create(key, "")

    # Verify that the tag was created with an empty value
    tag = tag_service.get(key)
    assert tag.value == ""


def test_get_tag_case_sensitivity(tag_service):
    """Test the case sensitivity of tag keys."""
    key_lower = "case_key"
    key_upper = "CASE_KEY"
    value_lower = "lower_case_value"
    value_upper = "upper_case_value"

    # Create both a lowercase and uppercase key
    tag_service.create(key_lower, value_lower)
    tag_service.create(key_upper, value_upper)

    # Verify that both tags exist and are distinct
    tag_lower = tag_service.get(key_lower)
    tag_upper = tag_service.get(key_upper)
    assert tag_lower.value == value_lower
    assert tag_upper.value == value_upper
    assert tag_lower.key != tag_upper.key


def test_modify_only_key(tag_service):
    """Test modifying only the key of an existing tag."""
    key = "original_key"
    value = "original_value"
    new_key = "new_only_key"

    # Create a tag and modify only its key
    tag_service.create(key, value)
    tag_service.modify(old_key=key, key=new_key)

    # Verify that the key was updated and value remains the same
    modified_tag = tag_service.get(new_key)
    assert modified_tag.key == new_key
    assert modified_tag.value == value


def test_modify_only_value(tag_service):
    """Test modifying only the value of an existing tag."""
    key = "modify_value_key"
    value = "original_value"
    new_value = "new_only_value"

    # Create a tag and modify only its value
    tag_service.create(key, value)
    tag_service.modify(old_key=key, value=new_value)

    # Verify that the value was updated and the key remains the same
    modified_tag = tag_service.get(key)
    assert modified_tag.key == key
    assert modified_tag.value == new_value


def test_describe_empty(tag_service):
    """Test describing tags when no tags exist."""
    # Describe should return an empty list when no tags are present
    result = tag_service.describe()
    assert result == []


def test_create_tag_with_special_characters(tag_service):
    """Test creating a tag with special characters in the key and value."""
    key = "key_with_special_!@#$%^&*()"
    value = "value_with_special_{}[]<>"
   
    # Verify that the tag was created with the special characters
    with pytest.raises(ValueError):
        tag_service.create(key, value)


def test_create_tag_with_long_key(tag_service):
    """Test creating a tag with a very long key."""
    long_key = "k" * 256
    value = "long_key_value"
    with pytest.raises(ValueError):
        tag_service.create(long_key, value)


def test_create_tag_with_long_value(tag_service):
    """Test creating a tag with a very long value."""
    key = "long_value_key"
    long_value = "v" * 1024
    tag_service.create(key, long_value)

    # Verify that the tag with the long value was created
    tag = tag_service.get(key)
    assert tag.key == key
    assert tag.value == long_value


def test_modify_with_invalid_old_key(tag_service):
    """Test modifying a tag with an invalid original key."""
    invalid_key = "invalid_key"
    with pytest.raises(KeyError):
        tag_service.modify(old_key=invalid_key, key="new_key", value="new_value")


def test_delete_all_tags(tag_service):
    """Test deleting all tags and ensuring the memory is empty."""
    # Create a few tags
    tag_service.create("key1", "value1")
    tag_service.create("key2", "value2")
    tag_service.create("key3", "value3")

    # Delete all tags
    tag_service.delete("key1")
    tag_service.delete("key2")
    tag_service.delete("key3")

    # Describe should return an empty list
    assert tag_service.describe() == []


def test_modify_tag_to_empty_key(tag_service):
    """Test modifying a tag's key to an empty key, which should raise an error."""
    key = "non_empty_key"
    value = "some_value"
    tag_service.create(key, value)

    with pytest.raises(ValueError):  # Adjust based on your system's behavior
        tag_service.modify(old_key=key, key="")


def test_create_tag_with_whitespace_key(tag_service):
    """Test creating a tag with a key containing only whitespace."""
    with pytest.raises(ValueError):  # Adjust based on your system's behavior
        tag_service.create("   ", "value_with_whitespace_key")


def test_modify_tag_to_whitespace_key(tag_service):
    """Test modifying a tag's key to a key containing only whitespace."""
    key = "valid_key"
    value = "some_value"
    tag_service.create(key, value)

    with pytest.raises(ValueError):  # Adjust based on your system's behavior
        tag_service.modify(old_key=key, key="   ")
