from typing import List
import DBSecurityGroupService


class DBSecurityGroupController:
    
    def __init__(self, service:DBSecurityGroupService):
        self.service = service
        
    def authorize_db_security_group_ingress(self, db_security_group_name: str, cidrip:str = None, 
                                            ec2_security_group_id:str = None, ec2_security_group_name:str = None,
                                            ec2_security_group_owner_id:str = None):
        pass
    
    def create_db_security_group(self, db_security_group_description: str, db_security_group_name: str, 
                                 tags: List[Tag] = None):
        return self.service.create(db_security_group_description, db_security_group_name, tags)
    
    def delete_db_security_group(self, db_security_group_name: str):
        return self.service.delete(db_security_group_name)
    
    def describe_db_security_groups(self, db_security_group_name: str = None, filters:List[Filter] = None, 
                                    marker:str = None, max_records:int = 100):
        return self.service.describe(db_security_group_name, filters, marker, max_records)
    
    def revoke_db_security_group_ingress(self, db_security_group_name:str, cidrip: str = None, 
                                         ec2_security_group_id:str = None, ec2_security_group_name:str = None,
                                         ec2_security_group_owner_id:str = None):
        pass
    
    
        
    