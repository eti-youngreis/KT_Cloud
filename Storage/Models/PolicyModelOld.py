
from typing import Dict, Optional
class PolicyModel:
    def __init__(self, bucket_name: str, major_bucket_version: str, policy_content: str, policy_name: str, tags: Optional[Dict] = None):
        self.bucket_name = bucket_name
        self.major_bucket_version = major_bucket_version
        self.policy_content = policy_content
        self.policy_name = policy_name
        self.tags = tags
        self.available = True

    def to_dict(self) -> Dict:
        return {
            "bucketName": self.bucket_name,
            "majorBucketVersion": self.major_bucket_version,
            "policyContent": self.policy_content,
            "policyName": self.policy_name,
            "tags": self.tags,
            "available": self.available
        }
