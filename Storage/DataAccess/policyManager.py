import json
import os
from typing import Dict,  Optional
from Models.PolicyModel import PolicyModel
from Models.PermissionModel import Permission

class PolicyStorage:
    def __init__(self, file_path: str="D:\\בוטקמפ\\server\\metadata.json"):
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
        print(policy)
        if policy["policyName"] in data["server"]["policies"]:
            raise ValueError(f"Policy '{policy.policy_name}' already exists.")
        data["server"]["policies"][policy["policyName"]] = policy
        self._save_data(data)

    def delete(self, policy_name: str):
        """Delete an existing policy and remove references from users."""
        data = self._load_data()

        # Remove policy from "server" -> "policies"
        if policy_name in data["server"]["policies"]:
            del data["server"]["policies"][policy_name]
        else:
            raise ValueError(f"Policy '{policy_name}' does not exist.")

        self._save_data(data)

    def update(self, policy: PolicyModel):
        """Update an existing policy."""
        data = self._load_data()
        if policy.policy_name not in data["server"]["policies"]:
            raise ValueError(f"Policy '{policy.policy_name}' does not exist.")
        data["server"]["policies"][policy.policy_name] = policy.to_dict()
        self._save_data(data)

    def select(self, policy_name: str) -> Optional[PolicyModel]:
        """Select a policy by name."""
        data = self._load_data()
        policy_data = data.get("server", {}).get("policies", {}).get(policy_name)
        print(policy_data)
        if policy_data:
            permissions = [p for p in policy_data.get('permissions', [])]
            return PolicyModel(policy_name, policy_data['version'], permissions)
        return None

    def list_all(self) -> Dict[str, PolicyModel]:
        """List all policies."""
        data = self._load_data()
        policies = {}
        for policy_name, policy_data in data.get("server", {}).get("policies", {}).items():
            permissions = [Permission.from_dict(p) for p in policy_data.get('permissions', [])]
            policies[policy_name] = PolicyModel(policy_name, policy_data['version'], permissions)
        return policies
