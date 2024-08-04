import aiofiles
import os
import json
from datetime import datetime

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

    async def check_permissions(self, key, version_id, by_pass_governance_retention, is_sync=True):
        metadata = self.get_metadata(key)
        if metadata and version_id in metadata['versions']:
            version_metadata = metadata['versions'][version_id]
            legal_hold = version_metadata.get('LegalHold', {}).get('Status', 'OFF')
            retention_mode = version_metadata.get('Retention', {}).get('Mode', 'NONE')
            retain_until_date = version_metadata.get('Retention', {}).get('RetainUntilDate', None)

            # Check Legal Hold
            if legal_hold == 'ON':
                raise PermissionError(f"Version {version_id} of object {key} is under legal hold and cannot be deleted")

            # Check Retention
            if retention_mode == 'COMPLIANCE' and not by_pass_governance_retention:
                if retain_until_date and datetime.utcnow() < datetime.fromisoformat(
                        retain_until_date.replace('Z', '+00:00')):
                    raise PermissionError(
                        f"Version {version_id} of object {key} is under Compliance Retention and cannot be deleted until {retain_until_date}")

            return True
        return False





    #def has_delete_permission(self, key, version_id):
    #    metadata = self.get_metadata(key)
     #   if metadata:
    #        if version_id in metadata['versions']:
     #           legal_hold_status = metadata['versions'][version_id].get('LegalHold', {}).get('Status', 'OFF')
     #           return legal_hold_status == 'OFF'
     #   return False

# דוגמה לשימוש במחלקת MetadataManager
metadata_manager = MetadataManager("C:/task/metadata.json")

# הוספת גרסאות לדוגמה


# קבלת הגרסה העדכנית ביותר
#latest_version = metadata_manager.get_latest_version('C:/task/tile1.txt')
#print(f'The latest version ID for C:/task/f1.txt is {latest_version}')
