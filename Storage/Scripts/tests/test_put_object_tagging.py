
import os
import pytest
import asyncio
from PyStorage import PyStorage  

functions = PyStorage()

@pytest.fixture
def create_test_file(tmp_path):
    file_path = tmp_path / "test_file.txt"
    with open(file_path, 'w') as f:
        f.write("This is a test file.")
    return file_path

@pytest.fixture
def create_metadata_file(tmp_path):
    metadata_file_path = tmp_path / "metadata.json"
    with open(metadata_file_path, 'w') as f:
        f.write("{}")
    return metadata_file_path

@pytest.fixture
def setup_metadata_manager(create_metadata_file):
    metadata = functions.metadata_manager
    metadata.metadata_file = str(create_metadata_file)
    return metadata

def test_put_object_tagging(create_test_file, setup_metadata_manager):
    # Test putting object tagging with valid tags and version ID
    file_path = str(create_test_file)
    tags = [{'Key': 'Project', 'Value': 'Test'}]

    asyncio.run(functions.put_object_tagging(file_path, tags, version_id=123, async_flag=False))
    asyncio.run(functions.put_object_tagging(file_path, tags, version_id=123, async_flag=True))

    metadata_file = functions.metadata_manager.get_metadata(file_path)
    assert metadata_file.get('versions', {}).get(str(123), {}).get('TagSet') == tags

def test_put_object_tagging_file_without_tags(create_test_file, setup_metadata_manager):
    # Test putting object tagging with empty tags list and version ID
    file_path = str(create_test_file)
    tags = []

    asyncio.run(functions.put_object_tagging(file_path, tags, version_id=123, async_flag=False))
    asyncio.run(functions.put_object_tagging(file_path, tags, version_id=123, async_flag=True))

    metadata_file = setup_metadata_manager.get_metadata(file_path)
    assert metadata_file.get('versions', {}).get(str(123), {}).get('TagSet') == tags

def test_put_object_tagging_empty_values_sync(create_test_file, setup_metadata_manager):
    # Test putting object tagging with empty key and value in tags list
    file_path = str(create_test_file)
    tags = [{"Key": "", "Value": ""}]

    asyncio.run(functions.put_object_tagging(file_path, tags, version_id=123, async_flag=False))
    asyncio.run(functions.put_object_tagging(file_path, tags, version_id=123, async_flag=True))

    metadata_file = setup_metadata_manager.get_metadata(file_path)
    assert metadata_file.get('versions', {}).get(str(123), {}).get('TagSet') == tags

def test_put_object_tagging_single_key_empty_value(create_test_file, setup_metadata_manager):
    # Test putting object tagging with a single key and empty value
    file_path = str(create_test_file)
    tags = [{"Key": "SingleKey", "Value": ""}]

    asyncio.run(functions.put_object_tagging(file_path, tags, version_id=123, async_flag=False))
    asyncio.run(functions.put_object_tagging(file_path, tags, version_id=123, async_flag=True))

    metadata_file = setup_metadata_manager.get_metadata(file_path)
    assert metadata_file.get('versions', {}).get(str(123), {}).get('TagSet') == tags

def test_put_object_tagging_multiple_keys(create_test_file, setup_metadata_manager):
    # Test putting object tagging with multiple keys and values
    file_path = str(create_test_file)
    tags = [{"Key": "Key1", "Value": "Value1"}, {"Key": "Key2", "Value": "Value2"}]

    asyncio.run(functions.put_object_tagging(file_path, tags, version_id=123, async_flag=False))
    asyncio.run(functions.put_object_tagging(file_path, tags, version_id=123, async_flag=True))

    metadata_file = setup_metadata_manager.get_metadata(file_path)
    assert metadata_file.get('versions', {}).get(str(123), {}).get('TagSet') == tags

def test_put_object_tagging_invalid_arguments(create_test_file, setup_metadata_manager):
    # Test putting object tagging with invalid async_flag argument
    file_path = str(create_test_file)

    with pytest.raises(TypeError):
        asyncio.run(functions.put_object_tagging(file_path, tags=[], version_id=123, async_flag="a"))
        asyncio.run(functions.put_object_tagging(file_path, tags=[], version_id=123, async_flag="m"))

def test_put_object_tagging_empty_file_path(create_test_file, setup_metadata_manager):
    # Test putting object tagging with an empty file path and valid version ID
    file_path = str(create_test_file)

    asyncio.run(functions.put_object_tagging(file_path, tags=[], version_id=123, async_flag=False))
    asyncio.run(functions.put_object_tagging(file_path, tags=[], version_id=123, async_flag=True))

    metadata_file = setup_metadata_manager.get_metadata(file_path)
    assert metadata_file.get('versions', {}).get(str(123), {}).get('TagSet') == []

def test_put_object_tagging_large_metadata(create_test_file, setup_metadata_manager):
    # Test putting object tagging with a large number of tags
    file_path = str(create_test_file)
    large_tags = [{"Key": f"Key{i}", "Value": f"Value{i}"} for i in range(1000)]

    asyncio.run(functions.put_object_tagging(file_path, large_tags, version_id=123, async_flag=False))
    asyncio.run(functions.put_object_tagging(file_path, large_tags, version_id=123, async_flag=True))

    metadata_file = setup_metadata_manager.get_metadata(file_path)
    assert metadata_file.get('versions', {}).get(str(123), {}).get('TagSet') == large_tags

def test_put_object_tagging_special_characters(create_test_file, setup_metadata_manager):
    # Test putting object tagging with tags containing special characters
    file_path = str(create_test_file)
    tags = [{"Key": "Key!@#$%^&*()", "Value": "Value!@#$%^&*()"}]

    asyncio.run(functions.put_object_tagging(file_path, tags, version_id=123, async_flag=False))
    asyncio.run(functions.put_object_tagging(file_path, tags, version_id=123, async_flag=True))

    metadata_file = setup_metadata_manager.get_metadata(file_path)
    assert metadata_file.get('versions', {}).get(str(123), {}).get('TagSet') == tags

def test_put_object_tagging_duplicate_keys(create_test_file, setup_metadata_manager):
    # Test putting object tagging with duplicate keys
    file_path = str(create_test_file)
    tags = [{"Key": "DuplicateKey", "Value": "Value1"}, {"Key": "DuplicateKey", "Value": "Value2"}]

    asyncio.run(functions.put_object_tagging(file_path, tags, version_id=123, async_flag=False))
    asyncio.run(functions.put_object_tagging(file_path, tags, version_id=123, async_flag=True))

    metadata_file = setup_metadata_manager.get_metadata(file_path)
    assert metadata_file.get('versions', {}).get(str(123), {}).get('TagSet') == tags



def test_put_object_tagging_no_version_id(create_test_file, setup_metadata_manager):
    # Test putting object tagging without specifying a version ID
    file_path = str(create_test_file)
    tags = [{"Key": "Project", "Value": "Test"}]

    asyncio.run(functions.put_object_tagging(file_path, tags, async_flag=False))
    asyncio.run(functions.put_object_tagging(file_path, tags, async_flag=True))

    metadata_file = setup_metadata_manager.get_metadata(file_path)
    latest_version = setup_metadata_manager.get_latest_version(file_path)

    assert metadata_file.get('versions', {}).get(latest_version, {}).get('TagSet') == tags




