import asyncio
import hashlib
import json
import tempfile
from datetime import datetime
import os
import aiofiles

from metaDataManeger import MetadataManager
class  S3ClientSimulator:
    def __init__(self, metadata_file):
        self.metadata_manager = MetadataManager(metadata_file)
    async def get_object_lock_configuration(self, key, bucket=None, version_id=None, is_sync=True):
        metadata = self.metadata_manager.get_metadata(key)
        if metadata is None:
            raise FileNotFoundError(f"No metadata found for object {key}")

        # Determine which version to use
        if version_id is None:
            version_id = self.metadata_manager.get_latest_version(key)

        # Retrieve the Object Lock Configuration for the specified version
        version_data = metadata.get('versions', {}).get(version_id)
        if version_data is None:
            raise FileNotFoundError(f"No version found with ID {version_id} for object {key}")

        object_lock_config = version_data.get('ObjectLockConfiguration')
        if object_lock_config is None:
            raise KeyError(f"Object Lock Configuration not found for version {version_id} of object {key}")

        return object_lock_config
    async def get_object_torrent(self, key, bucket=None, request_payer=None, expected_bucket_owner=None, outfile=None, is_sync=True):

        # Retrieve metadata for the specified key
        metadata = self.metadata_manager.get_metadata(key)
        if metadata is None:
            raise FileNotFoundError(f"No metadata found for object {key}")

        # Create the torrent content
        torrent_content = {
            "key": key,
            "metadata": metadata,
            "torrent_info": "This is a simulated torrent file"
        }

        # Save to file if outfile is provided
        if outfile:
            with open(outfile, 'w', encoding='utf-8') as f:
                json.dump(torrent_content, f, indent=4, ensure_ascii=False)

        return torrent_content
    async def head_object(self, key, version_id=None, bucket=None, request_payer=None, expected_bucket_owner=None, is_sync=True):

        # Retrieve metadata for the specified key
        metadata = self.metadata_manager.get_metadata(key)
        if metadata is None:
            raise FileNotFoundError(f"No metadata found for object {key}")

        # Determine which version to use
        if version_id is None:
            # If no version is specified, use the latest version
            version_id = max(metadata["versions"].keys(), key=int)

        # Retrieve metadata for the specified version
        version_metadata = metadata["versions"].get(version_id)
        if version_metadata is None:
            raise FileNotFoundError(f"No version found with ID {version_id} for object {key}")

        # Extract the necessary information for the HeadObject response
        head_object_response = {
            "LastModified": version_metadata.get("LastModified"),
            "ContentLength": version_metadata.get("ContentLength"),
            "ETag": version_metadata.get("ETag"),
            "ContentType": version_metadata.get("ContentType"),
            "Metadata": version_metadata.get("Metadata", {})
        }

        return head_object_response

    async def put_object(self, key, content, content_length=None, metadata=None, content_type=None, is_sync=True):

        content_length = content_length or len(content)
        metadata = metadata or {}
        content_type = content_type or "application/octet-stream"

        # Determine the new version ID
        existing_metadata = self.metadata_manager.get_metadata(key)
        if existing_metadata:
            version_id = str(max(int(v) for v in existing_metadata["versions"]) + 1).zfill(2)
        else:
            version_id = "00"

        # Save the content to a temporary file
        temp_dir = tempfile.gettempdir()
        file_path = os.path.join(temp_dir, key)
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        async with aiofiles.open(file_path, 'wb') as f:
            await f.write(content)

        # Generate ETag
        etag = self.generate_etag(content)

        # Prepare metadata for the new version
        new_metadata = {
            "ContentLength": content_length,
            "ContentType": content_type,
            "ETag": etag,
            "Metadata": metadata,
            "LastModified": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        }

        await self.metadata_manager.update_metadata(key, version_id, new_metadata, is_sync)

        return {
            "ETag": etag
        }
    def generate_etag(self, content):
        # Generate a simple ETag based on content (here we just return a placeholder)
        return hashlib.md5(content).hexdigest()
    async def put_object_acl(self, key, acl=None, bucket=None, version_id=None, is_sync=True):
        # Retrieve metadata for the specified key
        metadata = self.metadata_manager.get_metadata(key)
        if metadata is None:
            raise FileNotFoundError(f"No metadata found for object {key}")

        # Determine which version to update
        if version_id is None:
            version_id = self.metadata_manager.get_latest_version(key)

        # Retrieve existing version metadata
        version_data = metadata.get('versions', {}).get(version_id)
        if version_data is None:
            raise FileNotFoundError(f"No version found with ID {version_id} for object {key}")

        # Update ACL settings
        if acl is not None:
            version_data['acl'] = acl

        # Save updated metadata
        await self.metadata_manager.update_metadata(key, version_id, version_data, is_sync)

        return {
            "VersionId": version_id,
            "ACL": version_data.get('acl', {})
        }

# דוגמה לשימוש במחלקת S3ClientSimulator
s3_simulator = S3ClientSimulator("D:/בוטקמפ/project vast/KT_Cloud/Storage/Scripts/metadata.json")

async def main():
    s3_simulator = S3ClientSimulator("D:/בוטקמפ/project vast/KT_Cloud/Storage/Scripts/metadata.json")
    try:
        # Fetching object lock configuration
        lock_config = await s3_simulator.get_object_lock_configuration(
            'D:/בוטקמפ/project vast/KT_Cloud/Storage/Scripts/file.txt',
            is_sync=False
        )
        print(f'Object Lock Configuration: {lock_config}')
        torrent_content = await s3_simulator.get_object_torrent(
            'D:/בוטקמפ/project vast/KT_Cloud/Storage/Scripts/file.txt',
            bucket='my-bucket',
            request_payer='requester',
            expected_bucket_owner='123456789012',
            outfile='output.torrent',
            is_sync=False
        )
        print(f'Torrent Content: {torrent_content}')
    except FileNotFoundError as e:
        print(e)
    try:
        # קריאת פרטי אובייקט, עם או בלי גרסה
        head_object_response = await s3_simulator.head_object(
            key='D:/בוטקמפ/project vast/KT_Cloud/Storage/Scripts/file.txt',
            version_id='12554'
        )
        print(f'Head Object Response: {head_object_response}')
    except FileNotFoundError as e:
        print(f'Error: {e}')
    except KeyError as e:
        print(f'Error: {e}')
    except Exception as e:
        print(f'Unexpected error: {e}')
    # קרא לפונקציה put_object
    content = b"Hello, this is a new object content!"
    response = await s3_simulator.put_object(
        key="D:/tmp/new_file.txt",
        content=content,
        metadata={"custom-metadata": "value"},
        content_type="text/plain",
        is_sync=False
    )
    print("Put Object Response:", response)
    acl = {
        "owner": "user1",
        "permissions": {
            "read": ["user1", "user2"],
            "write": ["user1"]
        }
    }

    try:
        # Update object ACL
        acl_response = await s3_simulator.put_object_acl(
            key="D:/בוטקמפ/project vast/KT_Cloud/Storage/Scripts/file.txt",
            acl=acl,
            version_id="12554",
            is_sync=False
        )
        print("Put Object ACL Response:", acl_response)
    except FileNotFoundError as e:
        print(e)

asyncio.run(main())
