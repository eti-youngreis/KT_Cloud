import asyncio
from metadata import MetadataManager

class PyStorage:

    def __init__(self):
        # Initialize the metadata manager
        self.metadata_manager = MetadataManager()

    def sync_get_object_attributes(self, file_path):
        # Synchronously get object attributes by running the asynchronous function
        attributes = asyncio.run(self.get_object_attributes(file_path))
        return attributes

    async def get_object_attributes(self, key, version_id=None, async_flag=False):  
        # Get the metadata for the given key
        if async_flag:
            metadata = await asyncio.to_thread(self.metadata_manager.get_metadata, key)
        else:
            metadata = self.metadata_manager.get_metadata(key)   
              
        if metadata is None:
            raise FileNotFoundError(f'No metadata found for object {key}')

        # Determine which version to use
        if version_id is None:
            # If no version is specified, use the latest version
            version_id = max(metadata['versions'].keys(), key=int)

        # Get the metadata for the specified version
        version_metadata = metadata['versions'].get(str(version_id))

        if version_metadata is None:
            raise FileNotFoundError(f'No version found with ID {version_id} for object {key}')

        # Extract the required information for the response
        attributes = {
            'checksum': version_metadata.get('checksum'),
            'ETag': version_metadata.get('ETag'),
            'ObjectParts': version_metadata.get('ObjectParts'),
            'ObjectSize': version_metadata.get('ObjectSize'),
            'StorageClass': version_metadata.get('StorageClass', {})
        }

        return attributes

    def sync_put_object_tagging(self, file_path, tags, version_id=None):
        # Synchronously put object tagging by running the asynchronous function
        asyncio.run(self.put_object_tagging(file_path, tags, version_id))

    async def put_object_tagging(self, file_path, tags, version_id=None, async_flag=False):
        # Check if async_flag is a boolean
        if not isinstance(async_flag, bool):
            raise TypeError('async_flag must be a boolean')

        # Update metadata tags asynchronously or synchronously based on async_flag
        if async_flag:
            await self.metadata_manager.update_metadata_tags(file_path, version_id, {'TagSet': tags}, False)
        else:
            await self.metadata_manager.update_metadata_tags(file_path, version_id, {'TagSet': tags}, True)

        print(f'Tags for {file_path} have been saved.')

    def sync_get_object_tagging(self, file_path, version_id=None):
        # Synchronously get object tagging by running the asynchronous function
        tags = asyncio.run(self.get_object_tagging(file_path, version_id))
        return tags

    async def get_object_tagging(self, file_path, version_id=None, async_flag=False):
        # Check if async_flag is a boolean
        if not isinstance(async_flag, bool):
            raise TypeError('async_flag must be a boolean')

        # Get tags asynchronously or synchronously based on async_flag
        if async_flag:            
            tags = await asyncio.to_thread(self.metadata_manager.get_tags, file_path, version_id)
        else:
            tags = self.metadata_manager.get_tags(file_path, version_id)
        return tags
