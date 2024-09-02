from DataAccess import StorageManager
from typing import Dict, Any
import json
import aiofiles
import os
from .StorageManager import StorageManager
import datetime

class ObjectManager:

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
        if sync_flag:
            with open(self.metadata_file, 'w', encoding='utf-8') as f:
                json.dump(self.metadata, f, indent=4, ensure_ascii=False)
        else:
            async with aiofiles.open(self.metadata_file, 'w', encoding='utf-8') as f:
                await f.write(json.dumps(self.metadata, indent=4, ensure_ascii=False))

    async def create(self, bucket, key, data, body,version_id, object_metadata, sync_flag=True):
        object_metadata["versions"][version_id] = data
        self.get_bucket(bucket)["objects"][key] = object_metadata

        if sync_flag:
            with open(self.metadata_file, 'w', encoding='utf-8') as f:
                json.dump(self.metadata, f, indent=4, ensure_ascii=False)
        else:
            async with aiofiles.open(self.metadata_file, 'w', encoding='utf-8') as f:
                await f.write(json.dumps(self.metadata, indent=4, ensure_ascii=False))



    def get_bucket(self, bucket):
        bucketJson = self.metadata["server"]["buckets"].get(bucket, None)
        bucket = Bucket(bucketJson["name"], bucketJson["object"], bucketJson["creationDate"], bucketJson["policy"], bucketJson["acl"], bucketJson["Tag"], bucketJson["cors_configuration"])
        return bucket
    
    def get_object(self, bucket,key):
        bucket=self.get_bucket(bucket)
        if bucket:
            return bucket['objects'].get(key,None)
        else:
            raise FileNotFoundError(f'No bucket found')

    
    def get_buckets(self):
        return self.metadata['server']['buckets']

    def get_object_by_version(self, bucket, key, version_id=None):
        meta_data = self.get_versions(bucket, key).get(version_id, None)
        content = self.storage_manager.get(bucket, key, version_id)
        return {
            'Body': content,
            'ContentLength': meta_data.get('contentLength', len(content)),
            'ContentType': meta_data.get('contentType', 'application/octet-stream'),
            'ETag': meta_data.get('etag', ''),
            'Metadata': meta_data.get('metadata', {}),
            'LastModified': meta_data.get('lastModified', '')
        }

    def get_versions(self, bucket, key):
        # Retrieve versions for a given key
        metadata = self.get_bucket(bucket)['objects'][key]
        if metadata:
            return metadata.get('versions', {})
        return {}

        
    def get_latest_version(self, bucket, key):
        # Get the latest version for a given key
        metadata = self.get_object(bucket, key)
        if metadata and 'versions' in metadata:
            return max(metadata['versions'].keys(), key=int)
        else:
            raise FileNotFoundError(f"No versions found for object {key} in bucket {bucket}")

    def put(self, bucket, key, data, sync_flag):
        self.create(bucket, key, data, sync_flag)
        object_metadata = self.get_latest_version(bucket, key)
        version_id = str(len(object_metadata['versions']) + 1)
        self.storage_manager.create(bucket, key, version_id, data)
        
    #naive implementation

    def get_versioning_status(self, bucket):
        bucket_metadata = self.metadata['server']['buckets'].get(bucket, {})
        versioning_status = bucket_metadata.get('versioning', 'not enabled')
        return versioning_status

    #naive implementation
    async def delete_object(self, bucket, key, sync_flag=True):
        if bucket in self.metadata['server']['buckets'] and key in self.metadata['server']['buckets'][bucket]['objects']:
            del self.metadata['server']['buckets'][bucket]['objects'][key]
            if sync_flag:
                await self.update(True)
            else:
                await self.update(False)
            self.storage_manager.delete(bucket,key)

    #naive implementation
    async def put_deleteMarker(self, bucket, key,  version, sync_flag=True):
        if bucket not in self.metadata['server']['buckets']:
            self.metadata['server']['buckets'][bucket] = {'objects': {}}
        if key not in self.metadata['server']['buckets'][bucket]['objects']:
            self.metadata['server']['buckets'][bucket]['objects'][key] = {'versions': {}}
        self.metadata['server']['buckets'][bucket]['objects'][key]['deleteMarker'] =version
        if sync_flag:
            await self.update(True)
        else:
            await self.update(False)

        self.storage_maneger.encript_version(bucket,key,version)


    async def copy_object(self, source_bucket, source_key, destination_bucket, destination_key,version_id=None, sync_flag=True):
        source_metadata = self.get_versions(source_bucket, source_key)
        if not source_metadata:
            raise FileNotFoundError(f'Source object {source_bucket}/{source_key} not found')      
        if version_id is None:
            version_id = self.get_latest_version(source_bucket, source_key)
        source_version_metadata = source_metadata['versions'][version_id]

        # Prepare destination metadata
        destination_metadata = source_version_metadata.copy()
        destination_metadata['etag'] = 'newetag'  # Generate a new ETag as necessary
        destination_metadata['lastModified'] = datetime.utcnow().isoformat() + 'Z'
         
        # Update destination metadata
        await self.write_metadata_to_object(destination_bucket, destination_key, version_id, destination_metadata,sync_flag=sync_flag)
        self.storage_maneger.copy(source_bucket, source_key, destination_bucket, destination_key, version_id)

    async def read(self, bucket: str, key: str, version_id = None):
        try:
            if version_id is None:
                version_id = self.get_latest_version(bucket, key)
            
            content = self.storage_manager.get(bucket, key, version_id).content
            return content
        except FileNotFoundError as e:
            print(f"Error reading object: {str(e)}")
            return b""
