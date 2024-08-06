import asyncio
from metadata import MetadataManager

class PyStorage:

    def __init__(self):
        # Initialize the metadata manager
        self.metadata_manager = MetadataManager()


    async def get_object_attributes(self,bucket, key, version_id=None,MaxParts=None,PartNumberMarker =None,SSECustomerAlgorithm =None,SSECustomerKey =None,SSECustomerKeyMD5=None,RequestPayer=None,ExpectedBucketOwner =None,ObjectAttributes=None, sync_flag=False):  
        if sync_flag:
            metadata = self.metadata_manager.get_bucket_metadata(bucket, key)
        else:
            metadata = await asyncio.to_thread(self.metadata_manager.get_bucket_metadata, bucket, key)

        if metadata is None:
            raise FileNotFoundError(f'No metadata found for object {key}')

        # Use the latest version if no version ID is specified
        if version_id is None:
            version_id = max(metadata['versions'].keys(), key=int)

        # Get version metadata
        version_metadata = metadata['versions'].get(str(version_id))
        if version_metadata is None:
            raise FileNotFoundError(f'No version found with ID {version_id} for object {key}')

        # Extract and return object attributes
        attributes = {
            'checksum': version_metadata.get('checksum'),
            'ETag': version_metadata.get('ETag'),
            'ObjectParts': version_metadata.get('ObjectParts'),
            'ObjectSize': version_metadata.get('ObjectSize'),
            'StorageClass': version_metadata.get('StorageClass', {})
        }

        # Limit ObjectParts to MaxParts if specified
        if MaxParts is not None:
            object_parts = attributes.get('ObjectParts', [])
            if object_parts:
                attributes['ObjectParts'] = object_parts[:MaxParts]

        return attributes




    async def put_object_tagging(self,bucket, key,tags, version_id=None, ContentMD5=None,ChecksumAlgorithm=None,ExpectedBucketOwner=None,RequestPayer=None, sync_flag=True):
        
        if not isinstance(sync_flag, bool):
            raise TypeError('sync_flag must be a boolean')

        # Initialize metadata for the key if not present
        if key not in self.metadata_manager.metadata:
            self.metadata_manager.metadata[key] = {'versions': {}}

        versions = self.metadata_manager.get_versions(bucket, key)
        version_id_str = str(version_id)

        if version_id_str in versions:
            # Update TagSet for existing version
            versions[version_id_str]['TagSet'] = tags['TagSet']
        elif version_id:
            # Add new version with tags
            versions[version_id_str] = tags
        else:
            latest_version = self.metadata_manager.get_latest_version(key)
            if latest_version:
                versions[str(latest_version)]['TagSet'] = tags['TagSet']
            else:
                versions['0'] = tags
        
        # Save metadata
        if sync_flag:
            await self.metadata_manager.save_metadata(True)
        else:
            await self.metadata_manager.save_metadata(False)



    
    async def get_object_tagging(self,bucket, key,ExpectedBucketOwner=None,RequestPayer=None, version_id=None,sync_flag=True):
        if not isinstance(sync_flag, bool):
            raise TypeError('sync_flag must be a boolean')

        # Fetch metadata for the key
        if sync_flag:
            metadata = self.metadata_manager.get_bucket_metadata(bucket, key)
        else:
            metadata = await asyncio.to_thread(self.metadata_manager.get_bucket_metadata, bucket, key)

        versions = self.metadata_manager.get_versions(bucket, key)
        if metadata is None or versions is None:
            return []

        version_id_str = str(version_id)
        
        if version_id_str in versions:
            return versions[version_id_str].get('TagSet', [])
        
        elif not version_id:
            # Get TagSet from latest version if version ID is not provided
            latest_version = self.metadata_manager.get_latest_version(bucket, key)
            if latest_version:
                return versions[str(latest_version)].get('TagSet', [])
        
        return []
