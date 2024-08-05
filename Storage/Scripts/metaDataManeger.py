import asyncio
import os
import json
import aiofiles
from datetime import datetime
class MetadataManager:
    def __init__(self, metadata_file='D:\בוטקמפ\server/metadata.json'):
        self.metadata_file = metadata_file
        self.metadata = self.load_metadata()
    def load_metadata(self):
        if os.path.exists(self.metadata_file):
            with open(self.metadata_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        else:
            return {"server": {"buckets": {}}}
    async def save_metadata(self, is_sync=True):
        if is_sync:
            with open(self.metadata_file, 'w', encoding='utf-8') as f:
                json.dump(self.metadata, f, indent=4, ensure_ascii=False)
        else:
            async with aiofiles.open(self.metadata_file, 'w', encoding='utf-8') as f:
                await f.write(json.dumps(self.metadata, indent=4, ensure_ascii=False))
    def get_bucket_metadata(self, bucket, key):
        return self.metadata["server"]["buckets"].get(bucket, {}).get("objects", {}).get(key, None)
    async def update_metadata(self, bucket, key, version_id, data, is_sync=True):
        if bucket not in self.metadata["server"]["buckets"]:
            self.metadata["server"]["buckets"][bucket] = {"objects": {}}
        if key not in self.metadata["server"]["buckets"][bucket]["objects"]:
            self.metadata["server"]["buckets"][bucket]["objects"][key] = {'versions': {}}
        self.metadata["server"]["buckets"][bucket]["objects"][key]['versions'][version_id] = data
        if is_sync:
            await self.save_metadata(True)
        else:
            await self.save_metadata(False)
    async def delete_version(self, bucket, key, version_id, is_sync=True):
        bucket_data = self.metadata["server"]["buckets"].get(bucket, {})
        if key in bucket_data.get("objects", {}) and version_id in bucket_data["objects"][key]["versions"]:
            del self.metadata["server"]["buckets"][bucket]["objects"][key]["versions"][version_id]
            if not self.metadata["server"]["buckets"][bucket]["objects"][key]["versions"]:
                del self.metadata["server"]["buckets"][bucket]["objects"][key]
            if is_sync:
                await self.save_metadata(True)
            else:
                await self.save_metadata(False)
            return True
        return False
    async def delete_object(self, bucket, key, is_sync=True):
        if bucket in self.metadata["server"]["buckets"] and key in self.metadata["server"]["buckets"][bucket]["objects"]:
            del self.metadata["server"]["buckets"][bucket]["objects"][key]
            if is_sync:
                await self.save_metadata(True)
            else:
                await self.save_metadata(False)
            return True
        return False
    def get_latest_version(self, bucket, key):
        metadata = self.get_bucket_metadata(bucket, key)
        if metadata and "versions" in metadata:
            return max(metadata["versions"].keys(), key=int)
        else:
            raise FileNotFoundError(f"No versions found for object {key} in bucket {bucket}")
    async def check_permissions(self, bucket, key, version_id, by_pass_governance_retention, is_sync=True):
        metadata = self.get_bucket_metadata(bucket, key)
        if metadata and version_id in metadata['versions']:
            version_metadata = metadata['versions'][version_id]
            legal_hold = version_metadata.get('legalHold', False)
            retention_mode = version_metadata.get('retention', {}).get('mode', 'NONE')
            retain_until_date = version_metadata.get('retention', {}).get('retainUntilDate', None)
            # Check Legal Hold
            if legal_hold:
                raise PermissionError(f"Version {version_id} of object {key} in bucket {bucket} is under legal hold and cannot be deleted")
            # Check Retention
            if retention_mode == 'COMPLIANCE' and not by_pass_governance_retention:
                if retain_until_date and datetime.utcnow() < datetime.fromisoformat(
                        retain_until_date.replace('Z', '+00:00')):
                    raise PermissionError(
                        f"Version {version_id} of object {key} in bucket {bucket} is under Compliance Retention and cannot be deleted until {retain_until_date}")
            return True
        return False


# async def main():
#     # Create an instance of MetadataManager
#     metadata_manager = MetadataManager()
#     # Load existing metadata
#     print("Existing metadata:")
#     print(metadata_manager.metadata)
#     # Add a new version of an object
#     new_version_data = {
#         "etag": "newetag",
#         "size": 1234,
#         "lastModified": "2024-08-05T12:00:00Z",
#         "isLatest": True,
#         "acl": {
#             "owner": "user4",
#             "permissions": ["READ", "WRITE"]
#         },
#         "legalHold": False,
#         "retention": {
#             "mode": "GOVERNANCE",
#             "retainUntilDate": "2025-08-05T12:00:00Z"
#         },
#         "tagSet": [
#             {
#                 "key": "Key1",
#                 "value": "Value1"
#             },
#             {
#                 "key": "Key2",
#                 "value": "Value2"
#             }
#         ],
#         "contentLength": 36,
#         "contentType": "text/plain",
#         "metadata": {
#             "custom-metadata": "value"
#         }
#     }
#     await metadata_manager.update_metadata("bucket1", "object4.txt", "1", new_version_data, is_sync=True)
#     print("Metadata after adding a new version:")
#     print(metadata_manager.metadata)
#     # Get the latest version of an object
#     latest_version = metadata_manager.get_latest_version("bucket1", "object4.txt")
#     print(f"The latest version of object4.txt in bucket1 is: {latest_version}")
#     # Check permissions for deleting a specific version
#     try:
#         await metadata_manager.check_permissions("bucket1", "object4.txt", latest_version, by_pass_governance_retention=False)
#         print(f"Permissions check passed for version {latest_version} of object4.txt in bucket1")
#     except PermissionError as e:
#         print(f"Permission error: {e}")
#     # Delete a specific version
#     delete_version_result = await metadata_manager.delete_version("bucket1", "object4.txt", latest_version, is_sync=True)
#     if delete_version_result:
#         print(f"Version {latest_version} of object4.txt in bucket1 deleted successfully")
#     else:
#         print(f"Failed to delete version {latest_version} of object4.txt in bucket1")
#     # Delete an entire object
#     delete_object_result = await metadata_manager.delete_object("bucket1", "object4.txt", is_sync=True)
#     if delete_object_result:
#         print(f"Object object4.txt in bucket1 deleted successfully")
#     else:
#         print(f"Failed to delete object object4.txt in bucket1")
#     print("Metadata after deleting object4.txt:")
#     print(metadata_manager.metadata)
# if __name__ == "__main__":
#     asyncio.run(main())
