from DataAccess import StorageManager
from typing import Dict, Any
import json
import aiofiles
import os

from Models.VesionModel import Version
from .StorageManager import StorageManager
class VersionManager:
    def __init__(self, metadata_file="s3 project/KT_Cloud/Storage/server/metadata.json"):
        self.metadata_file = metadata_file
        self.metadata = self.load_metadata()
        self.storage_maneger = StorageManager()

    def load_metadata(self):
        # Load metadata from file if it exists, otherwise return default structure
        if os.path.exists(self.metadata_file):
            with open(self.metadata_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        else:
            return {"server": {"buckets": {}}}

    async def update(self, sync_flag=True):
        # Save metadata either synchronously or asynchronously
        pass

    async def create(self, bucket, key, data, body,version_id, object_metadata, sync_flag=True):
        pass


    def get(self, bucket:str, key:str, version:str) -> Version:
        return self.metadata["server"]["buckets"][bucket]["objects"][key]["versions"].get(version,None)

    def put(self, bucket, key, data, sync_flag):
        pass

    def get_versioning_status(self, bucket):
        bucket_metadata = self.metadata["server"]["buckets"].get(bucket, {})
        versioning_status = bucket_metadata.get("versioning", "not enabled")
        return versioning_status

