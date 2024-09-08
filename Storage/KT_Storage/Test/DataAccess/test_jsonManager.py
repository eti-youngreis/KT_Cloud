import pytest
import sys
import os
import asyncio
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'DataAccess')))
from JsonManager import JsonManager

@pytest.fixture
def json_manager():

    return JsonManager(info_file="s3 project/KT_Cloud/Storage/server/metadata.json")

def test_get_info_valid_key(json_manager):

    result = json_manager.get_info("server.users.user1.bucket1.Versioning")
    assert result == "ENABLED"

def test_get_info_nested_valid_key(json_manager):

    result = json_manager.get_info("server.users.user1.bucket1.objects.file.versions.1.acl.owner")
    assert result == "user4"

def test_get_info_invalid_key(json_manager):

    result = json_manager.get_info("server.users.user1.bucket1.nonexistentKey")
    assert result is None

def test_get_info_partial_key(json_manager):

    result = json_manager.get_info("server.policies.myPolicy.version")
    assert result == "2012-10-17"

def test_get_info_empty_key(json_manager):

    result = json_manager.get_info("")
    assert result is None

async def test_update_info_valid_key(json_manager):

    result = await json_manager.update_info("server.users.user7.buckets.bucket1.policies",['a','b'])
    assert result == True

async def test_update_info_nested_valid_key(json_manager):

    result = await json_manager.update_info("server.users.user1.bucket1.objectLock.lockConfiguration.mode",'COMPLIANCE')
    assert result == True

async def test_update_info_invalid_key(json_manager):

    result = await json_manager.update_info("server.users.user1.bucket1.nonexistentKey","qqq")
    assert result == False


async def test_update_info_empty_key(json_manager):

    result = await json_manager.update_info("",'aaa')
    assert result == True

async def test_insert_info_valid_key(json_manager):

    result = await json_manager.insert_info("server.users.user5",{"buckets.bucket1.policies": [
                    "myPolicy",
                    "policy2"
                ]})
    assert result == True

async def test_insert_info_nested_valid_key(json_manager):

    result = await json_manager.insert_info("server.users.user1.bucket1.objectLock.lockConfiguration.mode",'COMPLIANCE')
    assert result == True

async def test_insert_info_invalid_key(json_manager):

    new_value = {
        "ETag": "etag123",
        "size": 5678,
        "lastModified": "2024-09-01T12:00:00Z",
        "isLatest": True,
        "acl": {
            "owner": "user5",
            "permissions": ["READ", "WRITE"]
        }
    }
    result = await json_manager.insert_info("server.users.non_existent_user.bucket1.objects.new_file.txt.versions.1", new_value)
    assert result == False

async def test_insert_info_empty_key(json_manager):

    result = await json_manager.insert_info("",'aaa')
    assert result == True

async def test_delete_info_valid_key(json_manager):

    result = await json_manager.delete_info("server.users.user1.bucket1.objects.aa")
    assert result == True

async def test_delete_info_nested_valid_key(json_manager):

    result = await json_manager.delete_info("server.users.user1.bucket1.objects.file.txt")
    assert result == True

async def test_delete_info_invalid_key(json_manager):

    result = await json_manager.delete_info("server.users.user1.bucket1.nonexistentKey")
    assert result == False

async def test_delete_info_empty_key(json_manager):

    result = await json_manager.delete_info("")
    assert result == True

