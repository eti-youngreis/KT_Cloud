from typing import List


class DBSubnetGroup:
    
    def __init__(self, db_subnet_group_arn: str = None, db_subnet_group_description: str = None,
                 db_subnet_group_name: str = None, subnet_group_status: str = None, 
                 subnets: List[Subnet] = None, supported_network_types: List[str] = None, vpc_id:str = None):
        self.db_subnet_group_arn = db_subnet_group_arn
        self.db_subnet_group_description = db_subnet_group_description
        self.db_subnet_group_name = db_subnet_group_name
        self.subnet_group_status = subnet_group_status
        self.subnets = subnets if subnets is not None else []
        self.supported_network_types = supported_network_types if supported_network_types is not None else []
        self.vpc_id = vpc_id
        
    def to_dict(self):
        return {}