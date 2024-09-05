
from typing import Dict, Optional, Set 
from Storage.Models.ObjectModel import ObjectModel

class Bucket:
    def __init__(self, name: str, region, creationDate, policy, ACL, Tags, cors_configuration) -> None:
        self.name = name
        self.objects: Set[ObjectModel] = set()
        self.region = region
        self.creationDate = creationDate
        self.policy = policy
        self.ACL = ACL
        self.Tags = Tags
        self.cors_configuration = cors_configuration
        self.encrypt_mode = False
        
    # def is_encrypted(self):
    #     return self.encrypt_mode
    
    # def update_encryption(self, is_encrypted):
    #     self.encrypt_mode = is_encrypted
    # def to_dict(self):
    #     return{
    #         "name":self.name,
    #         "Objects":[obj.to_dict for obj in self.objects],
    #         "Region":self.region,
    #         "CreationDate":self.CreationDate,
    #         "Policy":self.policy,
    #         "name":self.name,
    #         "name":self.name,


    #     }

