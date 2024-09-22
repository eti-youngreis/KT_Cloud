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
    """Fixture to provide a TagObjectService instance with a test tag."""
    tag_service = TagObjectService(db_file=":memory:")
    # Create a default tag for testing
    default_key = "test_key"
    default_value = "test_value"
    tag_service.create(default_key, default_value)
    return tag_service, default_key, default_value

def test_create_tag(tag_service):
    """Test the creation of a new tag."""
    tag_service, _, _ = tag_service
    new_key = "new_key"
    new_value = "new_value"
    tag_service.create(new_key, new_value)

    # Assert that the tag is created and exists in memory
    tag = tag_service.get(new_key)
    assert tag is not None
    assert tag.key == new_key
    assert tag.value == new_value

def test_get_tag(tag_service):
    """Test retrieving a tag by key."""
    tag_service, key, value = tag_service

    # Retrieve the tag and verify its correctness
    tag = tag_service.get(key)
    assert tag is not None
    assert tag.key == key
    assert tag.value == value

def test_raise_KeyError_where_get_not_exist_tag(tag_service):
    tag_service, _, _ = tag_service
    key_that_not_exist = "key_that_not_exist"
    with pytest.raises(KeyError):
        tag_service.get(key_that_not_exist)

def test_modify_tag(tag_service):
    """Test modifying an existing tag."""
    tag_service, key, value = tag_service
    new_key = "new_test_key"
    new_value = "new_test_value"

    # Modify the tag
    tag_service.modify(old_key=key, key=new_key, value=new_value)

    # Verify the tag was updated
    modified_tag = tag_service.get(new_key)
    assert modified_tag is not None
    assert modified_tag.key == new_key
    assert modified_tag.value == new_value

def test_modify_tag_key_to_existing_key(tag_service):
    """Test modifying a tag's key to an already existing key."""
    tag_service, key1, value1 = tag_service
    key2 = "existing_key_2"
    value2 = "value2"
    tag_service.create(key2, value2)

    # Attempt to modify key1 to key2, which already exists
    with pytest.raises(Exception):  # Adjust based on how your system handles this
        tag_service.modify(old_key=key1, key=key2)

def test_get_after_modify_key(tag_service):
    """Test that the original key is no longer accessible after modifying the key."""
    tag_service, key, value = tag_service
    new_key = "new_mod_key"
    tag_service.modify(old_key=key, key=new_key)

    # The old key should raise a KeyError
    with pytest.raises(KeyError):
        tag_service.get(key)

def test_delete_tag(tag_service):
    """Test deleting an existing tag."""
    tag_service, key, value = tag_service

    # Delete the tag and verify it's gone
    tag_service.delete(key)

    with pytest.raises(KeyError):
        tag_service.get(key)

def test_delete_non_existent_tag(tag_service):
    """Test attempting to delete a non-existent tag."""
    tag_service, _, _ = tag_service
    non_existent_key = "non_existent_key"

    # Attempting to delete should not raise an error but should have no effect
    with pytest.raises(KeyError):
        tag_service.delete(non_existent_key)

def test_modify_non_existent_tag(tag_service):
    """Test modifying a tag that does not exist."""
    tag_service, _, _ = tag_service
    non_existent_key = "non_existent_key"

    # Modifying a non-existent tag should raise an error
    with pytest.raises(KeyError):
        tag_service.modify(old_key=non_existent_key, key="new_key", value="new_value")

def test_create_duplicate_tag(tag_service):
    """Test creating a tag with a duplicate key."""
    tag_service, key, value = tag_service
    duplicate_key = "duplicate_key"
    value1 = "value1"
    value2 = "value2"

    # Create the first tag
    tag_service.create(duplicate_key, value1)

    # Creating a second tag with the same key should raise an error or overwrite
    with pytest.raises(Exception):  # Modify this according to how duplicates are handled
        tag_service.create(duplicate_key, value2)

def test_create_with_empty_key(tag_service):
    """Test creating a tag with an empty key."""
    tag_service, _, _ = tag_service
    with pytest.raises(ValueError):  # Adjust based on the actual exception
        tag_service.create("", "value_with_empty_key")

def test_create_with_empty_value(tag_service):
    """Test creating a tag with an empty value."""
    tag_service, key, _ = tag_service

    with pytest.raises(ValueError):
        tag_service.create(key, "")
    

def test_get_tag_case_sensitivity(tag_service):
    """Test the case sensitivity of tag keys."""
    tag_service, _, _ = tag_service
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
    tag_service, key, value = tag_service
    new_key = "new_only_key"

    # Modify only its key
    tag_service.modify(old_key=key, key=new_key)

    # Verify that the key was updated and value remains the same
    modified_tag = tag_service.get(new_key)
    assert modified_tag.key == new_key
    assert modified_tag.value == value

def test_modify_only_value(tag_service):
    """Test modifying only the value of an existing tag."""
    tag_service, key, value = tag_service
    new_value = "new_only_value"

    # Modify only its value
    tag_service.modify(old_key=key, value=new_value)

    # Verify that the value was updated and the key remains the same
    modified_tag = tag_service.get(key)
    assert modified_tag.key == key
    assert modified_tag.value == new_value

def test_describe_empty(tag_service):
    """Test describing tags when no tags exist."""
    tag_service, _, _ = tag_service
    # Describe should return an empty list when no tags are present
    result = tag_service.describe()
    assert result == []

def test_create_tag_with_special_characters(tag_service):
    """Test creating a tag with special characters in the key and value."""
    tag_service, _, _ = tag_service
    key = "key_with_special_!@#$%^&*()"
    value = "value_with_special_{}[]<>"
    
    # Verify that the tag was created with the special characters
    with pytest.raises(ValueError):
        tag_service.create(key, value)

def test_create_tag_with_long_key(tag_service):
    """Test creating a tag with a very long key."""
    tag_service, _, _ = tag_service
    long_key = "k" * 256
    value = "long_key_value"
    with pytest.raises(ValueError):
        tag_service.create(long_key, value)

def test_create_tag_with_long_value(tag_service):
    """Test creating a tag with a very long value."""
    tag_service, key, _ = tag_service
    long_value = "v" * 1024
    with pytest.raises(ValueError):
        tag_service.create(key, long_value)




def test_modify_with_invalid_old_key(tag_service):
    """Test modifying a tag with an invalid original key."""
    tag_service, _, _ = tag_service
    invalid_key = "invalid_key"
    with pytest.raises(KeyError):
        tag_service.modify(old_key=invalid_key, key="new_key", value="new_value")

def test_delete_all_tags(tag_service):
    """Test deleting all tags and ensuring the memory is empty."""
    tag_service, _, _ = tag_service
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
    tag_service, key, value = tag_service

    with pytest.raises(ValueError):  # Adjust based on your system's behavior
        tag_service.modify(old_key=key, key="")

def test_create_tag_with_whitespace_key(tag_service):
    """Test creating a tag with a key containing only whitespace."""
    tag_service, _, _ = tag_service
    with pytest.raises(ValueError):  # Adjust based on your system's behavior
        tag_service.create("   ", "value_with_whitespace_key")

def test_modify_tag_to_whitespace_key(tag_service):
    """Test modifying a tag's key to a key containing only whitespace."""
    tag_service, key, value = tag_service

    with pytest.raises(ValueError):  # Adjust based on your system's behavior
        tag_service.modify(old_key=key, key="   ")
