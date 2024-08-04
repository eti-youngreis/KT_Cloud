import hashlib
import shutil
from datetime import datetime
import aiofiles
import os
import asyncio
import json
from MetaData import MetadataManager


class S3ClientSimulator:
    def __init__(self, metadata_file):
        self.metadata_manager = MetadataManager(metadata_file)

    async def get_object(self, key, version_id=None, is_sync=True):

        # find the metadata of the object
        metadata = self.metadata_manager.get_metadata(key)

        if metadata:
            if version_id is None:
                # find the current version
                version_id = self.metadata_manager.get_latest_version(key)

            versioned_key = self.get_versioned_key(key, version_id)
            if os.path.exists(versioned_key):
                if is_sync:
                    with open(versioned_key, 'r') as f:
                        return f.read()
                else:
                    async with aiofiles.open(versioned_key, 'r') as f:
                        return await f.read()
            else:
                raise FileNotFoundError(f"Version {version_id} of object {key} not found on disk")
        else:
            raise FileNotFoundError(f"Object {key} not found")

    async def get_object_acl(self, key, version_id=None, is_sync=True):

        # find the metadata of the object
        metadata = self.metadata_manager.get_metadata(key)
        if metadata:
            if version_id is None:
                version_id = await self.metadata_manager.get_latest_version(key, is_sync=is_sync)

            if version_id in metadata['versions']:
                acl = metadata['versions'][version_id].get('acl', None)
                if acl is not None:
                    return {
                        "Owner": acl.get("owner", "unknown"),
                        "Grants": acl.get("permissions", {})
                    }
                else:
                    raise ValueError(f"ACL for version {version_id} of object {key} not found")
            else:
                raise FileNotFoundError(f"Version {version_id} of object {key} not found")
        else:
            raise FileNotFoundError(f"Object {key} not found")

    async def delete_object(self, key, version_id=None, is_sync=True, by_pass_governance_retention=False):
        if version_id:
            if not await self.metadata_manager.check_permissions(key, version_id, by_pass_governance_retention,
                                                                 is_sync=is_sync):
                return {
                    "DeleteMarker": False,
                    "VersionId": version_id,
                    "RequestCharged": "requester"
                }
            success = await self.metadata_manager.delete_version(key, version_id, is_sync=is_sync)
            if success:
                # find the path of the object of the version_id
                versioned_key = self.get_versioned_key(key, version_id)
                if os.path.exists(versioned_key):
                    if is_sync:
                        os.remove(versioned_key)
                    else:
                        await asyncio.get_event_loop().run_in_executor(None, os.remove, versioned_key)
                return {
                    "DeleteMarker": True,
                    "VersionId": version_id,
                    "RequestCharged": "requester"
                }
            else:
                raise FileNotFoundError(f"Version {version_id} of object {key} not found")
        else:
            # find the current version
            latest_version_id = self.metadata_manager.get_latest_version(key)
            if not await self.metadata_manager.check_permissions(key, latest_version_id, by_pass_governance_retention,
                                                                 is_sync=is_sync):
                return {
                    "DeleteMarker": False,
                    "VersionId": latest_version_id,
                    "RequestCharged": "requester"
                }
            success = await self.metadata_manager.delete_object(key, is_sync=is_sync)
            if success:
                # find the path of the object of the version_id
                versioned_key = self.get_versioned_key(key, latest_version_id)
                if os.path.exists(versioned_key):
                    if is_sync:
                        os.remove(versioned_key)
                    else:
                        await asyncio.get_event_loop().run_in_executor(None, os.remove, versioned_key)

                return {
                    "DeleteMarker": True,
                    "VersionId": latest_version_id,
                    "RequestCharged": "requester"
                }
            else:
                raise FileNotFoundError(f"Object {key} not found")

    async def delete_objects(self, keys, is_sync=True, by_pass_governance_retention=False):
        deleted = []
        errors = []
        for item in keys:
            key = item.get('Key')
            version_id = item.get('VersionId')
            try:
                result = await self.delete_object(key, version_id, is_sync=is_sync,
                                                  by_pass_governance_retention=by_pass_governance_retention)
                deleted.append({
                    'Key': key,
                    'VersionId': result.get('VersionId'),
                    'DeleteMarker': result.get('DeleteMarker'),
                    'DeleteMarkerVersionId': result.get('VersionId')
                })
            except (FileNotFoundError, PermissionError) as e:
                errors.append({
                    'Key': key,
                    'VersionId': version_id,
                    'Code': "Error",
                    'Message': str(e)
                })
        return {
            'Deleted': deleted,
            'Errors': errors,
            'RequestCharged': "requester"
        }

    def _calculate_etag(self, file_path):
        """Calculate the ETag of a file by its MD5 hash."""
        hash_md5 = hashlib.md5()
        if os.path.exists(file_path):
            with open(file_path, 'r') as f:
                for chunk in iter(lambda: f.read(4096), ""):
                    hash_md5.update(chunk)
        return hash_md5.hexdigest()

    async def copy_object(self, source_key, destination_path, destination_key, is_sync=True):

        if not isinstance(is_sync, bool):
            raise TypeError(f"is_sync must be of type bool, got {type(is_sync).__name__}")

        source_path = source_key
        destination_file_path = destination_path + '/' + destination_key

        # Check if the source file exists
        if not os.path.exists(source_path):
            raise FileNotFoundError(f"No such file: '{source_path}'")

        # Check if the destination file already exists
        if os.path.abspath(source_path) == os.path.abspath(destination_file_path):
            raise shutil.SameFileError(
                f"Source file '{source_path}' and destination file '{destination_file_path}' are the same")

        # Copy the source file to the destination path
        if is_sync:
            shutil.copy2(source_path, destination_file_path)
        else:
            await asyncio.get_event_loop().run_in_executor(None, shutil.copy2, source_path, destination_file_path)

        # load the metadata
        metadata = self.metadata_manager.metadata
        if source_key in metadata:
            # Copy metadata for the source key to the destination key
            data_to_add = {destination_file_path: metadata[source_key]}
        else:
            raise KeyError(f"No metadata found for source key: '{source_key}'")

        # update the metadata file
        data_to_add[destination_file_path]['LastModified'] = datetime.utcnow().isoformat() + 'Z'
        data_to_add[destination_file_path]['ETag'] = f"\"{self._calculate_etag(destination_file_path)}\""
        metadata.update(data_to_add)

        if is_sync:
            with open(self.metadata_manager.metadata_file, 'w') as file:
                json.dump(metadata, file, indent=4)
        else:
            async with aiofiles.open(self.metadata_manager.metadata_file, 'w') as file:
                await file.write(json.dumps(metadata, indent=4))

        return {
            'CopyObjectResult': {
                'ETag': data_to_add[destination_file_path]['ETag'],
                'LastModified': data_to_add[destination_file_path]['LastModified']
            }
        }

    def get_versioned_key(self, key, version_id):
        base, ext = os.path.splitext(key)
        return f"{base}-{version_id}{ext}"


def is_valid_type(key, type_check):
    if not isinstance(key, type_check):
        raise TypeError(f"type of {key} is Error- got {type(key).__name__}")


# Usage Example
s3_sim = S3ClientSimulator('C:/task/metadata.json')
res = asyncio.run(s3_sim.get_object('C:/task/tile1.txt', is_sync=True))
print(res)

acl_info = asyncio.run(s3_sim.get_object_acl('C:/task/tile1.txt', version_id='1', is_sync=False))
print(acl_info)

# res=asyncio.run(s3_sim.delete_object('C:/task/wu.txt',by_pass_governance_retention=True))
# print(res)
# objects_to_delete = [
#     {'Key': 'C:/task/tile1.txt'},
#     {'Key': 'C:/task/tile2.txt'}
# ]
# res=asyncio.run(s3_sim.delete_objects(objects_to_delete, True))
# print(res)

# res = asyncio.run(s3_sim.copy_object('C:/task/tile2.txt', 'C:/task', "wu.txt", is_sync=False))
# print(res)
