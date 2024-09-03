from typing import Dict, Any, Optional,Union,List
import json
import os
import datetime
import aiofiles

class JsonManager:
    def __init__(self, metadata_file="C:\\Users\\The user\\Downloads\\metadata.json"):
        self.metadata_file = metadata_file
        self.metadata = self._load_metadata()
        
    def _load_metadata(self) -> Dict[str, Any]:
        """Load metadata from file if it exists, otherwise return an empty dictionary."""
        if os.path.exists(self.metadata_file):
            with open(self.metadata_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        return {}
    
    async def _save_metadata(self,sync_flag)->Any:
        """Save the updated metadata to the file"""
        if sync_flag:
            with open(self.metadata_file, 'w', encoding='utf-8') as f:
                json.dump(self.metadata, f, indent=4, ensure_ascii=False)
        else:
            async with aiofiles.open(self.metadata_file, 'w', encoding='utf-8') as f:
                await f.write(json.dumps(self.metadata, indent=4, ensure_ascii=False))
    
    def get_metadata(self, key: str) -> dict:
        """
        Retrieve a value by a nested key from metadata.
        For example: jsonManager_object.get_metadata("server.users.user1.bucket1)
        -> Return bucket1 for user1.
        
        """

        keys = key.split(".")

        data = self.metadata
        for k in keys:
            if isinstance(data, dict) and k in data:
                data = data[k]
            else:
                return None
        return data
    
    
    async def update_metadata(self, key: str, value: Any, sync_flag:bool=True)->bool:
        """
        Update metadata and save it to file, synchronously or asynchronously.
        
        For example: await jsonManager_object.update_metadata("server.users.user7.buckets.bucket1.policies",['a','b'])
        -> Update the permissions list in Bucket1 for User7 to ['a','b'].
        
        """
        
        keys = key.split(".")

        # Update the metadata dictionary
        data = self.metadata
        for k in keys[:-1]:
            if isinstance(data, dict) and k in data:
                data = data[k]
            else:
                return False
        data[keys[-1]] = value
        
        # Save the updated metadata to the file
        await self._save_metadata(sync_flag)
        return True
    
    async def insert_metadata(self, key:str, value: Any={}, sync_flag:bool=True)->bool:
            """
            Insert metadata and save it to file, synchronously or asynchronously.
            
            For example: await jsonManager_object.insert_metadata("server.users.user7")
            -> Adding a new user named user7 with an empty variable under them.
            await jsonManager_object.insert_metadata("server.users.user7.buckets.bucket1",{"policies": [
                    "myPolicy",
                    "policy2"
                ]})
            -> Adding a bucket named bucket1 to user7 with an updated list of permissions.    
            """
            keys = key.split(".")

            # insert the metadata dictionary
            data = self.metadata
            for k in keys[:-1]:
                if k not in data or not isinstance(data[k], dict):
                    return False
                data = data[k]
            data[keys[-1]] = value

            # Save the updated metadata to the file
            await self._save_metadata(sync_flag)
            return True
                    
    async def delete_metadata(self, key: str, sync_flag:bool=True) -> bool:
        """
        Delete a value by a nested key from metadata.
        for example: await jsonManager_object.delete_metadata("server.users.user1.bucket1.objects.aa")
        ->Deleting the file aa located in bucket1 of user user1 if it exists.
        
        """

        keys = key.split(".")

        data = self.metadata
        for k in keys[:-1]:
            if isinstance(data, dict) and k in data:
                data = data[k]
            else:
                return False  # The key path does not exist

        # Try to delete the final key
        if isinstance(data, dict) and keys[-1] in data:
            del data[keys[-1]]
            
            # Save the updated metadata to the file
            await self._save_metadata(sync_flag)
            
            return True
        else:
            return False  # The final key does not exist
        