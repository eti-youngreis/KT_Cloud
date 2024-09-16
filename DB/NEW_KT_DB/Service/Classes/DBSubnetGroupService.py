# service/db_subnet_group_service.py

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))

from typing import Any, Dict, List
from DataAccess.DBSubnetGroupManager import DBSubnetGroupManager, SubnetManager
from Models.DBSubnetGroupModel import DBSubnetGroup, Subnet
from sqlalchemy.orm import Session


class DBSubnetGroupService:
    def __init__(self, session: Session): 
        self.db_subnet_group_repo = DBSubnetGroupManager(session)
        self.subnet_repo = SubnetManager(session)

    def create_db_subnet_group(self, db_subnet_group_name: str, db_subnet_group_description: str, vpc_id: str, subnet_ids: List[str], db_subnet_group_arn: str):
        """Create a new DBSubnetGroup with associated subnets."""
        subnets = [self.subnet_repo.get_by_id(subnet_id) for subnet_id in subnet_ids]
        if None in subnets:
            raise ValueError("One or more subnets not found.")

        db_subnet_group = DBSubnetGroup(
            db_subnet_group_name=db_subnet_group_name,
            db_subnet_group_description=db_subnet_group_description,
            vpc_id=vpc_id,
            subnets=subnets,
            db_subnet_group_arn = db_subnet_group_arn,
            status = 'pending'
        )
        self.db_subnet_group_repo.create(db_subnet_group)

    def get_db_subnet_group(self, db_subnet_group_name: str) -> DBSubnetGroup:
        """Get DBSubnetGroup by name."""
        return self.db_subnet_group_repo.get_by_name(db_subnet_group_name)

    def delete_db_subnet_group(self, db_subnet_group_name: str):
        """Delete a DBSubnetGroup."""
        self.db_subnet_group_repo.delete(db_subnet_group_name)

    def modify_db_subnet_group(self, db_subnet_group_name: str, updates:Dict[str, Any]):
        for key, value in updates.items():
            if key == 'subnets':
                # Manage subnet associations
                new_subnets = [self.subnet_repo.get_by_id(subnet_id) for subnet_id in value]
                if None in new_subnets:
                    raise ValueError("One or more subnets not found.")
                updates['subnets'] = new_subnets

        self.db_subnet_group_repo.modify_by_id(db_subnet_group_name, updates)
        
    def list_db_subnet_groups(self) -> List[DBSubnetGroup]:
        """List all DBSubnetGroups."""
        return self.db_subnet_group_repo.get_all()

    def list_all_subnets(self) -> List[Subnet]:
        return self.subnet_repo.get_all()
    
    def create_subnet(self, subnet_id: str):
        """Create a new subnet."""
        subnet = Subnet(subnet_id=subnet_id, status='pending')
        self.subnet_repo.create(subnet)
        
    def delete_subnet(self, subnet_id: str):
        """Delete a subnet."""
        self.subnet_repo.delete(subnet_id)
        

        
    
    