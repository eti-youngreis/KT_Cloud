import json
import os
from typing import Dict, Optional, List
from Storage_UserAdministration.Models.PermissionModel import Permission, Action, Resource, Effect
from Storage_UserAdministration.Models.PolicyModel import PolicyModel

class PolicyManager:
    def __init__(self, file_path: str = "KT_Cloud\\server\\metadata.json"):
        self.file_path = file_path
        if not os.path.exists(self.file_path):
            with open(self.file_path, 'w') as file:
                json.dump({"server": {"policies": {}}, "users": {}}, file, indent=4)

    def _load_data(self) -> Dict:
        """Load all data from the JSON file."""
        with open(self.file_path, 'r') as file:
            return json.load(file)

    def _save_data(self, data: Dict):
        """Save all data to the JSON file."""
        with open(self.file_path, 'w') as file:
            json.dump(data, file, indent=4)

    def exists(self, policy_name: str) -> bool:
        """Check if a policy exists."""
        data = self._load_data()
        return policy_name in data.get("server", {}).get("policies", {})

    def insert(self, policy: PolicyModel):
        """Insert a new policy."""
        data = self._load_data()
        if policy["policyName"] in data["server"]["policies"]:
            raise ValueError(f"Policy '{policy['policyName']}' already exists.")

        permission_ids = policy['permissions']  # הרשאות כבר מועברות כ-IDs, ולכן אין צורך במיפוי נוסף
        policy_data = {
            "version": policy["version"],
            "policyName": policy["policyName"],
            "permissions": permission_ids
        }
        data["server"]["policies"][policy["policyName"]] = policy_data
        self._save_data(data)

    def delete(self, policy_name: str):
        """Delete an existing policy and remove references from users."""
        data = self._load_data()
        if policy_name in data["server"]["policies"]:
            del data["server"]["policies"][policy_name]
        else:
            raise ValueError(f"Policy '{policy_name}' does not exist.")
        self._save_data(data)

    def update(self, policy: PolicyModel):
        data = self._load_data()
        # שימוש בגישה ישירה למאפיינים של האובייקט PolicyModel
        if policy.policy_name not in data["server"]["policies"]:
            raise KeyError(f"Policy '{policy.policy_name}' not found.")
        print("policy.to_dict()",policy.to_dict())
        # update the policy
        data["server"]["policies"][policy.policy_name] = policy.to_dict()
        self._save_data(data)
        return policy

    def select(self, policy_name: str) -> Optional[PolicyModel]:
        """Select a policy by name."""
        data = self._load_data()
        policy_data = data.get("server", {}).get("policies", {}).get(policy_name)
        if policy_data:
            permissions = [item for item in policy_data['permissions'] if isinstance(item, (int, float))]
            return PolicyModel(policy_name, policy_data['version'], permissions)
        return None

    def list_all(self) -> Dict[str, PolicyModel]:
        """List all policies."""
        data = self._load_data()
        policies = []
        for policy_name, policy_data in data.get("server", {}).get("policies", {}).items():
            permissions = [Permission.get_permission_by_id(p_id) for p_id in policy_data.get("permissions", {})]
            policies.append(PolicyModel(policy_name, policy_data.get("version", {}), permissions))
        return policies

