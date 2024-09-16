
import sys
import os

import sqlalchemy

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))


from typing import Any, List, Dict
from sqlalchemy.orm import Session
from Models.DBSubnetGroupModel import DBSubnetGroup, Subnet

class DBSubnetGroupManager:
    def __init__(self, session: Session):
        self.session = session

    def get_all(self) -> List[DBSubnetGroup]:
        """Retrieve all DBSubnetGroup records."""
        try:
            return self.session.query(DBSubnetGroup).all()
        except sqlalchemy.exc.PendingRollbackError:
            self.session.rollback()

    def get_by_name(self, db_subnet_group_name: str) -> DBSubnetGroup:
        """Retrieve a DBSubnetGroup by its name."""
        try:
            return self.session.query(DBSubnetGroup).filter_by(db_subnet_group_name=db_subnet_group_name).first()
        except sqlalchemy.exc.PendingRollbackError:
            print(f'rollback in get, couldn\'t get {db_subnet_group_name}')
            self.session.rollback()

    def create(self, db_subnet_group: DBSubnetGroup):
        """Insert a new DBSubnetGroup into the database."""
        try:
            self.session.add(db_subnet_group)
            self.session.commit()
        except sqlalchemy.exc.PendingRollbackError:
            print(f'rollback in create - didn\'t create {db_subnet_group}')
            self.session.rollback()

    def delete(self, db_subnet_group_name: str):
        """Delete a DBSubnetGroup by its name."""
        try:
            db_subnet_group = self.get_by_name(db_subnet_group_name)
            if db_subnet_group:
                self.session.delete(db_subnet_group)
                self.session.commit()
        except sqlalchemy.exc.PendingRollbackError:
            self.session.rollback()
            
    def modify(self, db_subnet_group: DBSubnetGroup):
        """Modify a DBSubnetGroup by its name."""
        try:
            db_subnet_group = self.get_by_name(db_subnet_group.db_subnet_group_name)
            if db_subnet_group:
                db_subnet_group.db_subnet_group_description = db_subnet_group.db_subnet_group_description
                db_subnet_group.vpc_id = db_subnet_group.vpc_id
                db_subnet_group.subnets = db_subnet_group.subnets
                self.session.commit()
        except sqlalchemy.exc.PendingRollbackError:
            self.session.rollback()
            
    def modify_by_id(self, db_subnet_group_name: str, updates: Dict[str, Any]):
        """Modify a DBSubnetGroup by its name."""
        try:
            db_subnet_group = self.get_by_name(db_subnet_group_name)
            if db_subnet_group:
                for key, value in updates.items():
                    setattr(db_subnet_group, key, value)
                self.session.commit()
        except sqlalchemy.exc.PendingRollbackError:
            self.session.rollback()

class SubnetManager:
    def __init__(self, session: Session):
        self.session = session

    def get_all(self) -> List[Subnet]:
        """Retrieve all Subnet records."""
        try:
            return self.session.query(Subnet).all()
        except sqlalchemy.exc.PendingRollbackError:
            self.session.rollback()

    def get_by_id(self, id: str) -> Subnet:
        """Retrieve a Subnet by its ID."""
        try:
            return self.session.query(Subnet).filter_by(subnet_id = id).first()
        except sqlalchemy.exc.PendingRollbackError:
            self.session.rollback()
            
    def create(self, subnet: Subnet):
        """Insert a new Subnet into the database."""
        try:
            self.session.add(subnet)
            self.session.commit()
        except sqlalchemy.exc.PendingRollbackError:
            self.session.rollback()
    
    def delete(self, subnet_id: str):
        """Delete a Subnet by its ID."""
        try:
            subnet = self.get_by_id(subnet_id)
            if subnet:
                self.session.delete(subnet)
                self.session.commit()
        except sqlalchemy.exc.PendingRollbackError:
            self.session.rollback()


    def modify(self, subnet: Subnet):
        try:
            db_subnet = self.get_by_id(subnet.subnet_id)
            if db_subnet:
                db_subnet.status = subnet.status
                self.session.commit()
        except sqlalchemy.exc.PendingRollbackError:
            self.session.rollback()
       