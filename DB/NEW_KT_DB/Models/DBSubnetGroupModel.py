from typing import List, Dict, Any
import ast
import json
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from DataAccess.ObjectManager import ObjectManager

class DBSubnetGroup:
    
    pk_column = 'db_subnet_group_name'
    table_name = 'db_subnet_groups'
    table_structure = f"""
        db_subnet_group_name primary key not null,
        db_subnet_group_description TEXT NOT NULL,
        vpc_id VARCHAR(255) NOT NULL,
        subnets JSONB DEFAULT '{{}}',
        db_subnet_group_arn VARCHAR(255),
        status VARCHAR(50) DEFAULT 'pending'
    """
    
    def __init__(self, **kwargs):
        try:
            print(kwargs)
            self.db_subnet_group_name = kwargs['db_subnet_group_name']
            self.db_subnet_group_description = kwargs['db_subnet_group_description']
            self.vpc_id = kwargs['vpc_id']
            self.subnets = kwargs.get('subnets', None)
            if not self.subnets:
                self.subnets = []
            if type(self.subnets) is not list:
                self.subnets = ast.literal_eval(self.subnets)
            self.db_subnet_group_arn = kwargs.get('db_subnet_group_arn', None)
            
        except KeyError as e:
            raise ValueError(f"Missing required attribute for DBSubnetGroup: {str(e)}")
        
        # Ideally:
        # self.db_subnet_group_arn should be dynamically created according to vpc-id, account-id and 
        # subnet-group-name, and then dynamically added to the routing table
        
        self.status = 'pending'
        self.pk_value = self.db_subnet_group_name
        
    def to_dict(self) -> Dict[str, Any]:
        return ObjectManager.convert_object_attributes_to_dictionary(
            db_subnet_group_name=self.db_subnet_group_name,
            db_subnet_group_description = self.db_subnet_group_description,
            vpc_id = self.vpc_id,
            subnets = self.subnets,
            db_subnet_group_arn = self.db_subnet_group_arn,
            status = self.status
        )

    def to_bytes(self):
        bytes = json.dumps(self.to_dict())
        bytes = bytes.encode('utf-8')
        return bytes

    def from_bytes_to_dict(bytes):
        return json.loads(bytes.decode('utf-8'))

    
    def to_sql(self):
        # Convert the model instance to a dictionary
        data_dict = self.to_dict()
        values = '(' + ", ".join(f'\'{json.dumps(v)}\'' if isinstance(v, dict) or isinstance(v, list) else f'\'{v}\'' if isinstance(v, str) else f'\'{str(v)}\''
                           for v in data_dict.values()) + ')'
        return values