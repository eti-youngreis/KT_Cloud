# from typing import Dict, Optional
# from DataAccess import DataAccessLayer
# from Models import PolicyModelOld
# from Abc import STO
# from Validation import is_valid_bucket_name, is_valid_policy_name
#
# class PolicyService(STO):
#     def __init__(self, dal: DataAccessLayer):
#         self.dal = dal
#
#     def create(self, bucket_name: str, major_bucket_version: str, policy_content: str, policy_name: str, tags: Optional[Dict] = None):
#         """Create a new policy."""
#         if not is_valid_bucket_name(bucket_name):
#             raise ValueError(f"Invalid bucket name: {bucket_name}")
#         if not is_valid_policy_name(policy_name):
#             raise ValueError(f"Invalid policy name: {policy_name}")
#
#         policy = PolicyModelOld(bucket_name, major_bucket_version, policy_content, policy_name, tags)
#         self.dal.insert('Policy', policy.to_dict())
#
#     def delete(self, policy_name: str):
#         """Delete an existing Policy."""
#         if not self.dal.exists('Policy', policy_name):
#             raise ValueError(f"Policy '{policy_name}' does not exist.")
#         self.dal.delete('Policy', policy_name)
#
#     def get(self, policy_name: str) -> Dict:
#         """Get a Policy."""
#         data = self.dal.select('Policy', policy_name)
#         if data is None:
#             raise ValueError(f"Policy '{policy_name}' does not exist.")
#         return data
#
#     def put(self, policy_name: str, updates: Dict):
#         """Put a Policy."""
#         if not self.dal.exists('Policy', policy_name):
#             raise ValueError(f"Policy '{policy_name}' does not exist.")
#
#         current_data = self.dal.select('Policy', policy_name)
#         if current_data is None:
#             raise ValueError(f"Policy '{policy_name}' does not exist.")
#
#         updated_data = {**current_data, **updates}
#         self.dal.update('Policy', policy_name, updated_data)
