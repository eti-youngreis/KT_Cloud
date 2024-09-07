# from Service import PolicyServiceOld
# class PolicyController:
#     def __init__(self, service: PolicyServiceOld):
#         self.service = service
#
#     def create_policy(self, bucket_name: str, major_bucket_version: str, policy_content: str, policy_name: str, tags: Optional[Dict] = None):
#         self.service.create(bucket_name, major_bucket_version, policy_content, policy_name, tags)
#
#     def delete_policy(self, policy_name: str):
#         self.service.delete(policy_name)
