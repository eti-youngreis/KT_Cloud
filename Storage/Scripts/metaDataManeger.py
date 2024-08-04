import aiofiles
import os
import json

class MetadataManager:
    def __init__(self, metadata_file='metadata.json'):
        self.metadata_file = metadata_file
        self.metadata = self.load_metadata()

    def load_metadata(self):
        if os.path.exists(self.metadata_file):
            with open(self.metadata_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        else:
            return {}

    async def save_metadata(self, is_sync=True):
        if is_sync:
            with open(self.metadata_file, 'w', encoding='utf-8') as f:
                json.dump(self.metadata, f, indent=4, ensure_ascii=False)
        else:
            async with aiofiles.open(self.metadata_file, 'w', encoding='utf-8') as f:
                await f.write(json.dumps(self.metadata, indent=4, ensure_ascii=False))

    def get_metadata(self, key):
        return self.metadata.get(key, None)

    async def update_metadata(self, key, version_id, data, is_sync=True):
        if key not in self.metadata:
            self.metadata[key] = {'versions': {}}
        self.metadata[key]['versions'][version_id] = data
        if is_sync:
            await self.save_metadata(True)
        else:
            await self.save_metadata(False)

    async def delete_version(self, key, version_id, is_sync=True):
        if key in self.metadata and version_id in self.metadata[key]['versions']:
            del self.metadata[key]['versions'][version_id]
            if not self.metadata[key]['versions']:
                del self.metadata[key]
            if is_sync:
                await self.save_metadata(True)
            else:
                await self.save_metadata(False)
            return True
        return False

    async def delete_object(self, key, is_sync=True):
        if key in self.metadata:
            del self.metadata[key]
            if is_sync:
                await self.save_metadata(True)
            else:
                await self.save_metadata(False)
            return True
        return False

    def get_latest_version(self, key):
        metadata = self.get_metadata(key)
        if metadata and "versions" in metadata:
            return max(metadata["versions"].keys(), key=int)
        else:
            raise FileNotFoundError(f"No versions found for object {key}")
