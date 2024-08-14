import hashlib
import os
import sys
import asyncio
from datetime import datetime

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from Models.ObjectModel import ObjectModel
from DataAccess.ObjectManager import ObjectManager
from Service.Abc.STOE import STOE

class ObjectService(STOE):
    def __init__(self):
        self.object_manager = ObjectManager()

    async def get(self,obj:ObjectModel, version_id=None, flag_sync=True):
        bucket = obj.bucket
        key = obj.key
        if bucket not in self.object_manager.get_buckets() or key not in self.object_manager.get_bucket(bucket)['objects'] :
            raise FileNotFoundError("bucket or key not exist")
        if version_id is None:
            version_id = self.object_manager.get_latest_version(bucket, key)
        elif version_id not in self.object_manager.get_versions(bucket, key):
            raise FileNotFoundError("version id not exist")

        return self.object_manager.get_object(bucket, key, version_id)

    async def put(self, obj: ObjectModel, body, acl=None, metadata=None, content_type=None, flag_sync=True):
        bucket = obj.bucket
        key = obj.key
        try:
            etag = self.generate_etag(body)
            data = {
                "etag": etag,
                "size": len(body),
                "lastModified": datetime.utcnow().isoformat() + "Z",
                "isLatest": True,
                "acl": acl if acl else {"owner": "default_owner", "permissions": ["READ", "WRITE"]},
                "legalHold": {"Status": "OFF"},
                "retention": {"mode": "NONE"},
                "tagSet": [],
                "contentLength": len(body),
                "contentType": content_type if content_type else "application/octet-stream",
                "metadata": metadata if metadata else {}
            }
            if not self.object_manager.get_bucket(bucket):
                bucket_metadata = {'objects': {}}
                self.object_manager.metadata['server']['buckets'][bucket] = bucket_metadata
                object_metadata = {"versions": {}}
            else:
                object_metadata = self.object_manager.get_bucket(bucket)["objects"].get(key, {"versions": {}})

            version_id = str(len(object_metadata['versions']) + 1)
            for version in object_metadata["versions"].values():
                version["isLatest"] = False
            await self.object_manager.update()
            await self.object_manager.create(bucket, key, data,body,version_id, object_metadata, flag_sync)
        except FileNotFoundError as e:
            print(f"Error: The file or directory was not found: {e}")
            raise
        except PermissionError as e:
            print(f"Error: Permission denied when accessing the file or directory: {e}")
            raise
        except OSError as e:
            print(f"OS error occurred: {e}")
            raise
        except Exception as e:
            print(f"An unexpected error occurred: {e}")

    def generate_etag(self, content):
            return hashlib.md5(content).hexdigest()

    async def create(self, body, acl=None, metadata=None, content_type=None, flag_sync=True):
        pass

    async def delete(self,obj:ObjectModel, version_id=None, flag_sync=True):
        bucket = obj.bucket
        key = obj.key
        version_status = self.object_manager.get_versioning_status(bucket)
        # If the versioning is not enabled, delete the object
        if version_status == 'not enabled':
            self.object_manager.delete_object(bucket,key)
            return {'DeleteMarker': False, 'VersionId': 'null'}
        # If the versionId is given or the versioning id suspended, put deleteMarker on specific version
        elif version_id or version_status == 'suspended':
            if version_status == 'suspended':
                version_id = self.object_manager.get_latest_version(bucket, key)
            await self.object_manager.put_deleteMarker(bucket, key, version_id, flag_sync=flag_sync)
            return {'DeleteMarker': True, 'VersionId': version_id}
        return {}

    def list(self, *args, **kwargs):
        """list storage object."""
        pass

    def head(self, *args, **kwargs):
        """check if object exists and is accessible with the appropriate user permissions."""
        pass

    async def copy_object(self, copy_source, is_sync=True):
        pass

    async def delete_objects(self, delete, is_sync=True):
        pass

    async def put_object_acl(self, acl, version_id=None, is_sync=True):
        pass

    async def get_object_acl(self, flag_sync=True):
        pass

    async def get_object_torrent(self, version_id=None):
        pass

    async def get_object_tagging(self, version_id=None, sync_flag=True):
        pass

    async def get_object_attributes(self, version_id=None, MaxParts=None, PartNumberMarker=None, sync_flag=False):
        pass

    async def get_object_lock_configuration(self):
        pass

    async def put_object_legal_hold(self, legal_hold_status, version_id=None, is_sync=True):
        pass

    async def get_object_legal_hold(self, version_id=None, is_async=True):
        pass

    async def get_object_retention(self, version_id=None, is_sync=True):
        pass

    async def put_object_retention(self, retention_mode, retain_until_date, version_id=None, is_sync=True):
        pass

    async def put_object_lock_configuration(self, object_lock_enabled, mode="GOVERNANCE", days=30, years=0,
                                            is_sync=True):
        pass

if __name__ == '__main__':
    async def main():
        res = ObjectService()
        obj = ObjectModel("bucket1", "file.txt")
        res1 = await res.get(obj, '1')
        print(res1)
        try:
            body = b"Hello, World! \n I write now I want to see if it's work"
            obj = ObjectModel("bucket3", "fff/file.txt")
            await res.put(obj, body)
        except Exception as e:
            print(e)

    asyncio.run(main())
