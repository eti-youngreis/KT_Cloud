import os
import pytest
import asyncio
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from PyStorage import PyStorage  # Ensure this is the correct import path

from metadata import MetadataManager

functions = PyStorage()

@pytest.fixture
def create_test_file(tmp_path):
    # Create a temporary test file
    file_path = tmp_path / "test_file.txt"
    with open(file_path, 'w') as f:
        f.write("This is a test file.")
    return file_path

@pytest.fixture
def create_metadata_file(tmp_path):
    # Create a temporary metadata file with an empty JSON object
    metadata_file_path = tmp_path / "metadata.json"
    with open(metadata_file_path, 'w') as f:
        f.write("{}")
    return metadata_file_path

@pytest.fixture
def setup_metadata_manager(create_metadata_file):
    # Set up the metadata manager with the test metadata file
    metadata = functions.metadata_manager
    metadata.metadata_file = str(create_metadata_file)
    return metadata

def test_get_object_tagging_file_with_tags(create_test_file, setup_metadata_manager):
    # Test getting tags from a file after adding them, both synchronously and asynchronously
    file_path = str(create_test_file)
    tags = [{'Key': 'Project', 'Value': 'Test'}]
    asyncio.run(functions.put_object_tagging(file_path, tags, version_id=456, async_flag=False))

    tags_sync = asyncio.run(functions.get_object_tagging(file_path, version_id=456, async_flag=False))
    tags_async = asyncio.run(functions.get_object_tagging(file_path, version_id=456, async_flag=True))

    assert tags_sync == tags
    assert tags_async == tags

def test_get_object_tagging_file_without_tags(create_test_file, setup_metadata_manager):
    # Test getting an empty list of tags from a file after clearing them, both synchronously and asynchronously
    file_path = str(create_test_file)
    tags = []
    asyncio.run(functions.put_object_tagging(file_path, tags, version_id=123, async_flag=False))

    tags_sync = asyncio.run(functions.get_object_tagging(file_path, version_id=123, async_flag=False))
    tags_async = asyncio.run(functions.get_object_tagging(file_path, version_id=123, async_flag=True))

    assert tags_sync == tags
    assert tags_async == tags

def test_get_object_tagging_empty_values_sync(create_test_file, setup_metadata_manager):
    # Test getting tags with empty key and value, both synchronously and asynchronously
    file_path = str(create_test_file)
    tags = [{"Key": "", "Value": ""}]
    asyncio.run(functions.put_object_tagging(file_path, tags, version_id=123, async_flag=False))

    result_sync = asyncio.run(functions.get_object_tagging(file_path, version_id=123, async_flag=False))
    result_async = asyncio.run(functions.get_object_tagging(file_path, version_id=123, async_flag=True))

    assert result_sync == tags
    assert result_async == tags

def test_get_object_tagging_single_key_empty_value(create_test_file, setup_metadata_manager):
    # Test getting tags with a single key and empty value, both synchronously and asynchronously
    file_path = str(create_test_file)
    tags = [{"Key": "SingleKey", "Value": ""}]
    asyncio.run(functions.put_object_tagging(file_path, tags, version_id=123, async_flag=False))

    result_sync = asyncio.run(functions.get_object_tagging(file_path, version_id=123, async_flag=False))
    result_async = asyncio.run(functions.get_object_tagging(file_path, version_id=123, async_flag=True))

    assert result_sync == tags
    assert result_async == tags

def test_get_object_tagging_multiple_keys(create_test_file, setup_metadata_manager):
    # Test getting tags with multiple key-value pairs, both synchronously and asynchronously
    file_path = str(create_test_file)
    tags = [{"Key": "Key1", "Value": "Value1"}, {"Key": "Key2", "Value": "Value2"}]
    asyncio.run(functions.put_object_tagging(file_path, tags, version_id=123, async_flag=False))

    result_sync = asyncio.run(functions.get_object_tagging(file_path, version_id=123, async_flag=False))
    result_async = asyncio.run(functions.get_object_tagging(file_path, version_id=123, async_flag=True))

    assert result_sync == tags
    assert result_async == tags

def test_get_object_tagging_invalid_arguments(create_test_file, setup_metadata_manager):
    # Test that invalid arguments for `async_flag` raise a TypeError
    file_path = str(create_test_file)
    with pytest.raises(TypeError):
        asyncio.run(functions.get_object_tagging(file_path, version_id=123, async_flag="a"))
        asyncio.run(functions.get_object_tagging(file_path, version_id=123, async_flag="m"))

def test_get_object_tagging_empty_file_path(create_test_file, setup_metadata_manager):
    # Test getting tags from an empty file path, expecting an empty dictionary for both sync and async
    file_path = str(create_test_file)
    tags_sync = asyncio.run(functions.get_object_tagging(file_path, version_id=123, async_flag=False))
    tags_async = asyncio.run(functions.get_object_tagging(file_path, version_id=123, async_flag=True))

    assert tags_sync == {}
    assert tags_async == {}

def test_get_object_tagging_non_existent_file():
    # Test getting tags from a non-existent file, expecting an empty dictionary for both sync and async
    file_path = "non_existent_file.txt"
    tags_sync = asyncio.run(functions.get_object_tagging(file_path, version_id=123, async_flag=False))
    tags_async = asyncio.run(functions.get_object_tagging(file_path, version_id=123, async_flag=True))
    
    assert tags_sync == {}
    assert tags_async == {}

def test_get_object_tagging_large_number_of_tags(create_test_file, setup_metadata_manager):
    # Test handling a large number of tags, both synchronously and asynchronously
    file_path = str(create_test_file)
    tags = [{"Key": f"Key{i}", "Value": f"Value{i}"} for i in range(100)]
    asyncio.run(functions.put_object_tagging(file_path, tags, version_id=123, async_flag=False))
    
    result_sync = asyncio.run(functions.get_object_tagging(file_path, version_id=123, async_flag=False))
    result_async = asyncio.run(functions.get_object_tagging(file_path, version_id=123, async_flag=True))
    
    assert result_sync == tags
    assert result_async == tags

def test_get_object_tagging_no_version_id(create_test_file, setup_metadata_manager):
    # Test getting tags from a file when no version ID is specified, should return tags for the latest version
    file_path = str(create_test_file)
    tags = [{"Key": "Project", "Value": "Test"}]

    asyncio.run(functions.put_object_tagging(file_path, tags, async_flag=False))

    result_sync = asyncio.run(functions.get_object_tagging(file_path, async_flag=False))

    latest_version = setup_metadata_manager.get_latest_version(file_path)

    result = asyncio.run(functions.get_object_tagging(file_path,latest_version, async_flag=False))

    assert result_sync == result

