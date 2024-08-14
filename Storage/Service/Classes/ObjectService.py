from ...Models.ObjectModel import ObjectModel
from ...DataAccess.ObjectManager import ObjectManager
from Abc import STOE

class ObjectService(STOE):
    def __init__(self,bucket_name, object_key):
        self.obj = ObjectModel(bucket_name,object_key)
        self.object_manager= ObjectManager()

    async def get(self, version_id=None, flag_sync=True):
        bucket=self.obj.bucket
        key=self.obj.key
        if not version_id:
            version_id=self.object_manager.get_latest_version(bucket,key)
        elif version_id not in self.object_manager.get_versions(bucket,key):
            raise FileNotFoundError("version id not exist")
        return self.object_manager.get_object(bucket,key,version_id)

    async def put(self,body, acl=None, metadata=None, content_type=None,flag_sync=True):
        pass

    async def create(self,body, acl=None, metadata=None, content_type=None,flag_sync=True):
        pass

    async def delete(self,  versionId=None, is_sync=True):
        pass

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

    async def put_object_acl(self, acl:Acl, version_id=None,is_sync=True):
        pass

    async def get_object_acl(self, flag_sync=True):
        pass

    async def get_object_torrent(self, version_id=None):
        pass

    async def get_object_tagging(self, version_id=None,sync_flag=True):
        pass

    async def get_object_attributes(self, version_id=None,MaxParts=None,PartNumberMarker =None, sync_flag=False):
        pass

    async def get_object_lock_configuration(self):
        pass

    async def put_object_legal_hold(self, legal_hold_status, version_id=None, is_sync=True):
        pass

    async def get_object_legal_hold(self,version_id=None, is_async=True):
        pass

    async def get_object_retention(self, version_id=None, is_sync=True):
        pass

    async def put_object_retention(self, retention_mode, retain_until_date, version_id=None, is_sync=True):
        pass

    async def put_object_lock_configuration(self,object_lock_enabled, mode="GOVERNANCE", days=30, years=0,is_sync=True):
        pass