import os
import json
import aiofiles
from datetime import datetime
class MetadataManager:
    
    def __init__(self,metadata_file='C:/Users/shana/Desktop/server/metadata.json'):
        self.metadata_file = metadata_file
        self.metadata = self.load_metadata()


    def load_metadata(self):
        if os.path.exists(self.metadata_file):
            with open(self.metadata_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        else:

            return {"server": {"buckets": {}}}
        

    async def save_metadata(self, sync_flag=True):
        if sync_flag:
            with open(self.metadata_file, 'w', encoding='utf-8') as f:
                json.dump(self.metadata, f, indent=4, ensure_ascii=False)
        else:
            async with aiofiles.open(self.metadata_file, 'w', encoding='utf-8') as f:
                await f.write(json.dumps(self.metadata, indent=4, ensure_ascii=False))


    def get_bucket_metadata(self, bucket, key):
        bucket_metadata = self.metadata["server"]["buckets"].get(bucket, {})
        object_metadata = bucket_metadata.get("objects", {}).get(key, None)
        return object_metadata
    

    # async def update_metadata(self, bucket, key, version_id, data, sync_flag=True):
    #     print("aaa")
    #     if bucket not in self.metadata["server"]["buckets"]:
    #         self.metadata["server"]["buckets"][bucket] = {"objects": {}}
    #     if key not in self.metadata["server"]["buckets"][bucket]["objects"]:
    #         self.metadata["server"]["buckets"][bucket]["objects"][key] = {'versions': {}}
    #     self.metadata["server"]["buckets"][bucket]["objects"][key]['versions'][version_id] = data
    #     if sync_flag:
    #         await self.save_metadata(True)
    #     else:
    #         await self.save_metadata(False)


    # async def delete_version(self, bucket, key, version_id, sync_flag=True):
    #     bucket_data = self.metadata["server"]["buckets"].get(bucket, {})
    #     if key in bucket_data.get("objects", {}) and version_id in bucket_data["objects"][key]["versions"]:
    #         del self.metadata["server"]["buckets"][bucket]["objects"][key]["versions"][version_id]
    #         if not self.metadata["server"]["buckets"][bucket]["objects"][key]["versions"]:
    #             del self.metadata["server"]["buckets"][bucket]["objects"][key]
    #         if sync_flag:
    #             await self.save_metadata(True)
    #         else:
    #             await self.save_metadata(False)
    #         return True
    #     return False
    

    # async def delete_object(self, bucket, key, sync_flag=True):
    #     if bucket in self.metadata["server"]["buckets"] and key in self.metadata["server"]["buckets"][bucket]["objects"]:
    #         del self.metadata["server"]["buckets"][bucket]["objects"][key]
    #         if sync_flag:
    #             await self.save_metadata(True)
    #         else:
    #             await self.save_metadata(False)
    #         return True
    #     return False
    

    def get_latest_version(self, bucket, key):
        metadata = self.get_bucket_metadata(bucket, key)
        if metadata and "versions" in metadata:
            return max(metadata["versions"].keys(), key=int)
        else:
            raise FileNotFoundError(f"No versions found for object {key} in bucket {bucket}")


    # async def check_permissions(self, bucket, key, version_id, by_pass_governance_retention, sync_flag=True):
    #     metadata = self.get_bucket_metadata(bucket, key)
    #     if metadata and version_id in metadata['versions']:
    #         version_metadata = metadata['versions'][version_id]
    #         legal_hold = version_metadata.get('legalHold', False)
    #         retention_mode = version_metadata.get('retention', {}).get('mode', 'NONE')
    #         retain_until_date = version_metadata.get('retention', {}).get('retainUntilDate', None)
    #         # Check Legal Hold
    #         if legal_hold:
    #             raise PermissionError(f"Version {version_id} of object {key} in bucket {bucket} is under legal hold and cannot be deleted")
    #         # Check Retention
    #         if retention_mode == 'COMPLIANCE' and not by_pass_governance_retention:
    #             if retain_until_date and datetime.utcnow() < datetime.fromisoformat(
    #                     retain_until_date.replace('Z', '+00:00')):
    #                 raise PermissionError(
    #                     f"Version {version_id} of object {key} in bucket {bucket} is under Compliance Retention and cannot be deleted until {retain_until_date}")
    #         return True
    #     return False
    def get_versions(self,bucket, key):
        # Retrieve versions for a given key
        metadata = self.get_bucket_metadata(bucket,key)
        if metadata:
            return metadata.get('versions', {})
        return {}
    
    async def update_metadata_tags(self,bucket, key, version_id, data, sync_flag=True):
        # Update only the TagSet field for a given key and version ID, preserving other fields
        if key not in self.metadata:
            self.metadata[key] = {'versions': {}}

        versions = self.get_versions(bucket,key)
        version_id_str = str(version_id)

        if version_id_str in versions:
            # Update the TagSet field if the version exists
            versions[version_id_str]['TagSet'] = data['TagSet']
        elif version_id:
            # Add a new version with the given data if the version does not exist
            versions[version_id_str] = data
        else:
            latest_version = self.get_latest_version(key)
            if latest_version:
                versions[str(latest_version)]['TagSet'] = data['TagSet']
            else:
                versions['0'] = data
                
        # Save metadata either synchronously or asynchronously
        if sync_flag:
            await self.save_metadata(True)
        else:
            await self.save_metadata(False)


    def get_tags(self,bucket, key, version_id=None):
        # Retrieve the TagSet for a given key and version ID
        metadata = self.get_bucket_metadata(bucket,key)
        versions = self.get_versions(bucket,key)
        if metadata is None or versions is None:
            return []

        version_id_str = str(version_id)
        
        if version_id_str in versions:
            return versions[version_id_str].get('TagSet', [])
        
        elif not version_id:
            # If the version ID is not found, try to get the latest version
                latest_version = self.get_latest_version(bucket,key)
                if latest_version:
                    return versions[str(latest_version)].get('TagSet', [])
                       
        return []
