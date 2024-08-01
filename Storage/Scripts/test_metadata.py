import pytest
import asyncio
from metadata import MetadataManager
import json

@pytest.fixture
def temp_metadata_file(tmp_path):
    # Fixture to create a temporary metadata file path for testing
    metadata_file = tmp_path / "temp_metadata.json"
    return str(metadata_file)

@pytest.fixture
def metadata_manager(temp_metadata_file):
    # Fixture to create an instance of MetadataManager with the temporary metadata file
    return MetadataManager(metadata_file=temp_metadata_file)

def test_load_metadata_file_exists(temp_metadata_file):
    # Test loading metadata from an existing file
    test_data = {"key1": "value1", "key2": "value2"}
    with open(temp_metadata_file, 'w') as f:
        json.dump(test_data, f)
    metadata_manager = MetadataManager(metadata_file=temp_metadata_file)
    metadata = metadata_manager.load_metadata()
    assert metadata == test_data

def test_load_metadata_file_not_exist(temp_metadata_file):
    # Test loading metadata when the file does not exist
    metadata_manager = MetadataManager(metadata_file=temp_metadata_file)
    metadata = metadata_manager.load_metadata()
    assert metadata == {}

@pytest.mark.asyncio
async def test_save_metadata_sync(temp_metadata_file):
    # Test saving metadata synchronously
    metadata_manager = MetadataManager(metadata_file=temp_metadata_file)
    await metadata_manager.save_metadata(is_sync=True)
    with open(temp_metadata_file, 'r') as f:
        metadata = json.load(f)
    assert metadata == {}

@pytest.mark.asyncio
async def test_save_metadata_async(temp_metadata_file):
    # Test saving metadata asynchronously
    metadata_manager = MetadataManager(metadata_file=temp_metadata_file)
    await metadata_manager.save_metadata(is_sync=False)
    with open(temp_metadata_file, 'r') as f:
        metadata = json.load(f)
    assert metadata == {}

