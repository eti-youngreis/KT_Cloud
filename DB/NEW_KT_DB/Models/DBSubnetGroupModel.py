from typing import List, Dict, Any
import ast
import json
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker

from DataAccess.ObjectManager import ObjectManager

Base = declarative_base()

association_table = Table(
    'db_subnet_group_subnet_association', Base.metadata,
    Column('db_subnet_group_name', String, ForeignKey('db_subnet_groups.db_subnet_group_name')),
    Column('subnet_id', String, ForeignKey('subnets.subnet_id'))
)

class DBSubnetGroup(Base):
    __tablename__ = 'db_subnet_groups'
    db_subnet_group_name = Column(String, primary_key = True)
    db_subnet_group_description = Column(String)
    vpc_id = Column(String)
    subnets = relationship("Subnet", secondary=association_table)
    status = Column(String)
    db_subnet_group_arn = Column(String)
    
class Subnet(Base):
    __tablename__ = 'subnets'
    subnet_id = Column(String, primary_key = True)
    status = Column(String)
    subnet_groups = relationship("DBSubnetGroup", secondary=association_table)

def get_engine():
    return create_engine('sqlite:///' + '../object_management_db.db')

def create_tables(engine):
    Base.metadata.create_all(engine)

def get_session():
    engine = get_engine()
    Session = sessionmaker(bind=engine)
    return Session()