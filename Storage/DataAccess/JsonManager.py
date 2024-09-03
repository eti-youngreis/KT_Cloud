from typing import Dict, Any, Optional,Union,List
import json
import os
import datetime
import aiofiles

class JsonManager:
    def __init__(self, info_file="s3 project/KT_Cloud/Storage/server/metadata.json"):
        self.info_file = info_file
        self.info = self._load_info()
        
    def _load_info(self) -> Dict[str, Any]:
        """Load info from file if it exists, otherwise return an empty dictionary."""
        if os.path.exists(self.info_file):
            with open(self.info_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        return {}
    
    async def _save_info(self,sync_flag)->Any:
        """Save the updated info to the file"""
        if sync_flag:
            with open(self.info_file, 'w', encoding='utf-8') as f:
                json.dump(self.info, f, indent=4, ensure_ascii=False)
        else:
            async with aiofiles.open(self.info_file, 'w', encoding='utf-8') as f:
                await f.write(json.dumps(self.info, indent=4, ensure_ascii=False))
    
    def get_info(self, key: str) -> dict:
        """
        Retrieve a value by a nested key from info.
        For example: jsonManager_object.get_info("server.users.user1.bucket1)
        -> Return bucket1 for user1.
        
        """

        keys = key.split(".")

        data = self.info
        for k in keys:
            if isinstance(data, dict) and k in data:
                data = data[k]
            else:
                return None
        return data
    
    
    async def update_info(self, key: str, value: Any, sync_flag:bool=True)->bool:
        """
        Update info and save it to file, synchronously or asynchronously.
        
        For example: await jsonManager_object.update_info("server.users.user7.buckets.bucket1.policies",['a','b'])
        -> Update the permissions list in Bucket1 for User7 to ['a','b'].
        
        """
        
        keys = key.split(".")

        # Update the info dictionary
        data = self.info
        for k in keys[:-1]:
            if isinstance(data, dict) and k in data:
                data = data[k]
            else:
                return False
        data[keys[-1]] = value
        
        # Save the updated info to the file
        await self._save_info(sync_flag)
        return True
    
    async def insert_info(self, key:str, value: Any={}, sync_flag:bool=True)->bool:
            """
            Insert info and save it to file, synchronously or asynchronously.
            
            For example: await jsonManager_object.insert_info("server.users.user7")
            -> Adding a new user named user7 with an empty variable under them.
            await jsonManager_object.insert_info("server.users.user7.buckets.bucket1",{"policies": [
                    "myPolicy",
                    "policy2"
                ]})
            -> Adding a bucket named bucket1 to user7 with an updated list of permissions.    
            """
            keys = key.split(".")

            # insert the info dictionary
            data = self.info
            for k in keys[:-1]:
                if k not in data or not isinstance(data[k], dict):
                    return False
                data = data[k]
            data[keys[-1]] = value

            # Save the updated info to the file
            await self._save_info(sync_flag)
            return True
                    
    async def delete_info(self, key: str, sync_flag:bool=True) -> bool:
        """
        Delete a value by a nested key from info.
        for example: await jsonManager_object.delete_info("server.users.user1.bucket1.objects.aa")
        ->Deleting the file aa located in bucket1 of user user1 if it exists.
        
        """

        keys = key.split(".")

        data = self.info
        for k in keys[:-1]:
            if isinstance(data, dict) and k in data:
                data = data[k]
            else:
                return False  # The key path does not exist

        # Try to delete the final key
        if isinstance(data, dict) and keys[-1] in data:
            del data[keys[-1]]
            
            # Save the updated info to the file
            await self._save_info(sync_flag)
            
            return True
        else:
            return False  # The final key does not exist
        