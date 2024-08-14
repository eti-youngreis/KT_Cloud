from typing import List


class DBSecurityGroup:
    
    def __init__(self, db_security_group_arn: str = None, db_security_group_description: str = None,
                 db_security_group_name: str = None, ec2_security_groups: List[EC2SecurityGroup] = None,
                 ip_ranges: List[IPRange] = None, owner_id: str = None, vpc_id: str = None):
        self.db_security_group_arn = db_security_group_arn
        self.db_security_group_description = db_security_group_description
        self.db_security_group_name = db_security_group_name
        self.ec2_security_groups = ec2_security_groups if ec2_security_groups is not None else []
        self.ip_ranges = ip_ranges if ip_ranges is not None else []
        self.owner_id = owner_id
        self.vpc_id = vpc_id
        
    def to_dict(self):
        return {}