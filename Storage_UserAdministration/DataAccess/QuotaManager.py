import json
import os
from typing import Dict,  Optional
from Models.QuotaModel import QuotaModel

class QuotaManager:
    def __init__(self, file_path: str="D:/boto3 project/metadata.json"):
        self.file_path = file_path
        if not os.path.exists(self.file_path):
            with open(self.file_path, 'w') as file:
                json.dump({"server": {"quotas": {}, "users": {}, "groups": {}, "roles": {}}, "users": {}}, file, indent=4)
        self.quota_entity_map = self._build_quota_entity_map()

    def _build_quota_entity_map(self):
        data = self.load_data()
        quota_entity_map = {}
        for quota_name, quota in data["server"]["quotas"].items():
            quota_entity_map[quota_name] = {
                "users": quota.get("users", []),
                "groups": quota.get("groups", []),
                "roles": quota.get("roles", [])
            }
        return quota_entity_map

    def assign_quota_to_entity(self, quota_name: str, entity_type: str, entity_id: str):
        if quota_name not in self.quota_entity_map:
            self.quota_entity_map[quota_name] = {"users": [], "groups": [], "roles": []}
        if entity_type in self.quota_entity_map[quota_name]:
            self.quota_entity_map[quota_name][entity_type].append(entity_id)
            self.save_quota_entity_map()

    def remove_quota_from_entity(self, quota_name: str, entity_type: str, entity_id: str):
        if quota_name in self.quota_entity_map and entity_type in self.quota_entity_map[quota_name]:
            if entity_id in self.quota_entity_map[quota_name][entity_type]:
                self.quota_entity_map[quota_name][entity_type].remove(entity_id)
                self.save_quota_entity_map()

    def save_quota_entity_map(self):
        data = self.load_data()
        for quota_name, entities in self.quota_entity_map.items():
            if quota_name in data["server"]["quotas"]:
                data["server"]["quotas"][quota_name]["users"] = entities["users"]
                data["server"]["quotas"][quota_name]["groups"] = entities["groups"]
                data["server"]["quotas"][quota_name]["roles"] = entities["roles"]
        self.save_data(data)

    def load_data(self) -> Dict:
        """Load all data from the JSON file."""
        with open(self.file_path, 'r') as file:
            return json.load(file)

    def save_data(self, data: Dict):
        """Save all data to the JSON file."""
        with open(self.file_path, 'w') as file:
            json.dump(data, file, indent=4)

    def exists(self, quota_name: str) -> bool:
        """Check if a Quota exists."""
        data = self.load_data()
        return quota_name in data.get("server", {}).get("quotas", {})

    def insert(self, quota: QuotaModel):
        """Insert a new quota."""
        data = self.load_data()
        if quota.name in data["server"]["quotas"]:
            raise ValueError(f"Quota '{quota.name}' already exists.")
        data["server"]["quotas"][quota.name] = quota.to_dict()
        self.save_data(data)

    def delete_quota(self, quota_name: str):
        """Delete an existing quota and remove references from users."""
        data = self.load_data()

        # Remove quota from "server" -> "quotas"
        if quota_name in data["server"]["quotas"]:
            del data["server"]["quotas"][quota_name]
        else:
            raise ValueError(f"Quota '{quota_name}' does not exist.")

        self.save_data(data)
        
    def delete(self, name:str, type:str, quota_name):
        data = self.load_data()
        lst = data["server"][type][name]["quota"]
        lst.remove(quota_name)
        print(lst)
        self.save_data(data)
        
    def update(self, quota: QuotaModel):
        """Update an existing quota."""
        data = self.load_data()
        if quota.name not in data["server"]["quotas"]:
            raise ValueError(f"Quota '{quota.name}' does not exist.")
        data["server"]["quotas"][quota.name] = quota.to_dict()
        self.save_data(data)

    def select(self, name: str) -> Optional[QuotaModel]:
        """Select a quota by name."""
        data = self.load_data()
        quota_data = data.get("server", {}).get("quotas", {}).get(name)
        if quota_data:
            return QuotaModel(
                name=quota_data['name'],
                resource_type=quota_data['resource_type'],
                restriction_type=quota_data['restriction_type'],
                limit=quota_data['limit'],
                period=quota_data['period'],
                usage=quota_data.get('usage', 0),
                users=quota_data.get('users', []),
                groups=quota_data.get('groups', []),
                roles=quota_data.get('roles', [])
            )
        return None

    def list_all(self) -> Dict[str, QuotaModel]:
        """List all quotas."""
        data = self.load_data()
        quotas = {}
        for quota_name, quota_data in data.get("server", {}).get("quotas", {}).items():
            quotas[quota_name] = QuotaModel(
                name=quota_data['name'],
                resource_type=quota_data['resource_type'],
                restriction_type=quota_data['restriction_type'],
                limit=quota_data['limit'],
                period=quota_data['period'],
                usage=quota_data.get('usage', 0),
                users=quota_data.get('users', []),
                groups=quota_data.get('groups', []),
                roles=quota_data.get('roles', [])
            )
        return quotas