
import asyncio
import os
import json
import aiofiles

class MetadataManager:
    def __init__(self, metadata_file='metadata.json'):
        # Initialize the MetadataManager with a metadata file
        self.metadata_file = metadata_file
        self.metadata = self.load_metadata()

    def load_metadata(self):
        # Load metadata from the file if it exists, otherwise return an empty dictionary
        if os.path.exists(self.metadata_file):
            with open(self.metadata_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        else:
            return {}

    # async def load_metadata(self, is_sync=True):
    # # Load metadata from the file either synchronously or asynchronously
    #     if is_sync:
    #         if os.path.exists(self.metadata_file):
    #             with open(self.metadata_file, 'r', encoding='utf-8') as f:
    #                 return json.load(f)
    #         else:
    #             return {}
    #     else:
    #         if os.path.exists(self.metadata_file):
    #             async with aiofiles.open(self.metadata_file, 'r', encoding='utf-8') as f:
    #                 content = await f.read()
    #                 return json.loads(content)
    #         else:
    #             return {}

    async def save_metadata(self, is_sync=True):
        # Save metadata to the file either synchronously or asynchronously
        if is_sync:
            with open(self.metadata_file, 'w', encoding='utf-8') as f:
                json.dump(self.metadata, f, indent=4, ensure_ascii=False)
        else:
            async with aiofiles.open(self.metadata_file, 'w', encoding='utf-8') as f:
                await f.write(json.dumps(self.metadata, indent=4, ensure_ascii=False))

    def get_metadata(self, key):
        # Retrieve metadata for a given key
        return self.metadata.get(key, None)

    async def update_metadata(self, key, version_id, data, is_sync=True):
        # Update metadata for a specific key and version ID
        if key not in self.metadata:
            self.metadata[key] = {'versions': {}}
        self.metadata[key]['versions'][version_id] = data
        if is_sync:
            await self.save_metadata(True)
        else:
            await self.save_metadata(False)

    async def update_metadata_tags(self, key, version_id, data, is_sync=True):
        # Update only the TagSet field for a given key and version ID, preserving other fields
        if key not in self.metadata:
            self.metadata[key] = {'versions': {}}

        versions = self.metadata[key]['versions']
        version_id_str = str(version_id)

        if version_id_str in versions:
            # Update the TagSet field if the version exists
            # if 'TagSet' in data:
            versions[version_id_str]['TagSet'] = data['TagSet']
        elif version_id:
            # Add a new version with the given data if the version does not exist
            versions[version_id_str] = data
        else:
            latest_version = self.get_latest_version(key)
            if latest_version:
                versions[str(latest_version)]['TagSet'] = data['TagSet']
            else:
                versions['0']=data
                
        # Save metadata either synchronously or asynchronously
        if is_sync:
            await self.save_metadata(True)
        else:
            await self.save_metadata(False)

    async def delete_version(self, key, version_id, is_sync=True):
        # Delete a specific version of the metadata for a given key
        if key in self.metadata and version_id in self.metadata[key]['versions']:
            del self.metadata[key]['versions'][version_id]
            if not self.metadata[key]['versions']:
                del self.metadata[key]
            if is_sync:
                self.save_metadata(True)
            else:
                await self.save_metadata(False)
            return True
        return False

    async def delete_object(self, key, is_sync=True):
        # Delete the metadata for a given key
        if key in self.metadata:
            del self.metadata[key]
            if is_sync:
                await self.save_metadata(True)
            else:
                await self.save_metadata(False)
            return True
        return False

    def get_latest_version(self, key):
        # Retrieve the latest version ID for a given key
        metadata = self.get_metadata(key)
        if metadata and "versions" in metadata:
            if metadata["versions"]:
                return max(metadata["versions"].keys(), key=int)
            else:
                return None
        else:
            raise FileNotFoundError(f"No versions found for object {key}")

    def get_tags(self, key, version_id=None):
        # Retrieve the TagSet for a given key and version ID
        metadata = self.metadata.get(key, None)
        if metadata is None or 'versions' not in metadata:
            return {}

        versions = metadata['versions']
        version_id_str = str(version_id)
        
        if version_id_str in versions:
            return versions[version_id_str].get('TagSet', {})
        else:
            # If the version ID is not found, try to get the latest version
            try:
                latest_version = self.get_latest_version(key)
                if latest_version:
                    return versions[str(latest_version)].get('TagSet', {})
            except FileNotFoundError:
                raise FileNotFoundError(f"No versions found for object {key}")
            
        return {}

