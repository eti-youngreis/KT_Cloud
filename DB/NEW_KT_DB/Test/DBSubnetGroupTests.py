from sqlite3 import IntegrityError
import pytest
import json

import sys
import os

import sqlalchemy
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))

from Service.Classes.DBSubnetGroupService import DBSubnetGroupService
from DataAccess.DBSubnetGroupManager import DBSubnetGroupManager
from Controller.DBSubnetGroupController import DBSubnetGroupController
from DataAccess.ObjectManager import ObjectManager
from Models.DBSubnetGroupModel import DBSubnetGroup
import sqlite3
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from Models.DBSubnetGroupModel import get_engine, get_session, create_tables
import time 

engine = get_engine()
create_tables(engine)
session = get_session()
    
db_subnet_group_controller = DBSubnetGroupController(DBSubnetGroupService(session))

@pytest.fixture
def clear_tables():
    # Connect to the SQLite database
    conn = sqlite3.connect('../object_management_db.db')
    cursor = conn.cursor()

    # Clear the table if it exists
    table_name = "db_subnet_groups"
    cursor.execute(f"DELETE FROM {table_name};")

    table_name = 'subnets'
    cursor.execute(f"delete from {table_name};")
    
    table_name = 'db_subnet_group_subnet_association'
    cursor.execute(f"delete from {table_name};")
    # Commit changes and close the connection
    conn.commit()
    conn.close()

    # Yield to allow tests to run
    yield

  
def test_create(clear_tables):
    db_subnet_group_controller.create_subnet('subnet_1')
    db_subnet_group_controller.create_subnet('subnet_2')
    # remove existing subnet group from previous tests
    db_subnet_group_controller.create_db_subnet_group(
        db_subnet_group_name='subnet_group_1',
        subnet_ids=['subnet_1', 'subnet_2'],
        db_subnet_group_description='Test subnet group',
        vpc_id='vpc-12345678',
        db_subnet_group_arn='arn:aws:rds:us-west-2:123456789012:subgrp:subnet_group_1'
    )

    # check that file was created (no error raised on get)
    # check that object was saved to management table (no error raised on get)
    db_subnet_group_controller.get_db_subnet_group('subnet_group_1')
    
    # check that file content is correct
    from_db = db_subnet_group_controller.get_db_subnet_group('subnet_group_1')
    # check values were stored correctly in management table as well as storage
    
    # group name
    assert from_db.db_subnet_group_name == 'subnet_group_1'
    
    # subnets
    for subnet in from_db.subnets:
        assert subnet.subnet_id in ['subnet_1', 'subnet_2']
    
    # description
    assert from_db.db_subnet_group_description == 'Test subnet group'
    
    # vpc_id
    assert from_db.vpc_id == 'vpc-12345678'
    
    # arn
    assert from_db.db_subnet_group_arn == 'arn:aws:rds:us-west-2:123456789012:subgrp:subnet_group_1'

        
def test_unique_constraint():
    with pytest.raises((sqlalchemy.exc.IntegrityError, sqlalchemy.exc.PendingRollbackError)):
        db_subnet_group_controller.create_db_subnet_group(
            db_subnet_group_name='subnet_group_1', 
            subnet_ids=['subnet_1', 'subnet_2'],
            db_subnet_group_description='Another subnet group with same name',
            vpc_id='vpc-87654321',
            db_subnet_group_arn='arn:aws:rds:us-west-2:123456789012:subgrp:subnet_group_1'
        )


def test_get():
    subnet_group_1 = db_subnet_group_controller.get_db_subnet_group('subnet_group_1')
    assert subnet_group_1 != None
    assert subnet_group_1.db_subnet_group_name == 'subnet_group_1'
    assert subnet_group_1.db_subnet_group_description == 'Test subnet group'
    assert subnet_group_1.vpc_id == 'vpc-12345678'  
    assert subnet_group_1.db_subnet_group_arn == 'arn:aws:rds:us-west-2:123456789012:subgrp:subnet_group_1'
    assert subnet_group_1.status == 'pending'
    for subnet in subnet_group_1.subnets:
        assert subnet.subnet_id in ['subnet_1', 'subnet_2']
    
def test_modify():
    db_subnet_group_controller.create_subnet('subnet_3')
    db_subnet_group_controller.modify_db_subnet_group(
        name='subnet_group_1',
        updates= {'subnets': ['subnet_1', 'subnet_3']}
    )
    
    from_db = db_subnet_group_controller.get_db_subnet_group('subnet_group_1')
    for subnet in from_db.subnets:
        assert subnet.subnet_id in ['subnet_1', 'subnet_3']
    assert from_db.db_subnet_group_name == 'subnet_group_1'
    assert from_db.db_subnet_group_description == 'Test subnet group'
    assert from_db.vpc_id == 'vpc-12345678'
    assert from_db.db_subnet_group_arn == 'arn:aws:rds:us-west-2:123456789012:subgrp:subnet_group_1'
    
    
# def test_describe():
#     subnet_group_1 = db_subnet_group_controller.describe_db_subnet_group('subnet_group_1')
#     assert type(subnet_group_1) == dict
#     assert type(subnet_group_1['subnets']) == list
#     assert type(subnet_group_1['subnets'][0]) == dict
    
    
    
def test_delete():
    db_subnet_group_controller.delete_db_subnet_group('subnet_group_1')
    assert db_subnet_group_controller.get_db_subnet_group('subnet_group_1') == None
    

@pytest.mark.parametrize("index", range(20))
def test_insert_many(index):
    db_subnet_group_controller.create_subnet(f'subnet_1{index}')
    db_subnet_group_controller.create_subnet(f'subnet_2{index}')
    db_subnet_group_controller.create_db_subnet_group(
        db_subnet_group_name=f'subnet_group_{index}',
        subnet_ids=[f'subnet_1{index}', f'subnet_2{index}'],
        db_subnet_group_description=f'Test subnet group {index}',
        vpc_id=f'vpc-1234567{index}',
        db_subnet_group_arn=f'arn:aws:rds:us-west-2:123456789012:subgrp:subnet_group_{index}'
    )
    
    # check that object was saved to management table (no error raised on get)
    db_subnet_group_controller.get_db_subnet_group(f'subnet_group_{index}')
    
    from_db = db_subnet_group_controller.get_db_subnet_group(f'subnet_group_{index}')
    # check values were stored correctly in management table as well as storage
    
    # group name
    assert from_db.db_subnet_group_name == f'subnet_group_{index}'
    
    for subnet in from_db.subnets:
        assert subnet.subnet_id in [f'subnet_1{index}', f'subnet_2{index}']
    
    # description
    assert from_db.db_subnet_group_description == f'Test subnet group {index}'
    
    # vpc_id
    assert from_db.vpc_id == f'vpc-1234567{index}'
    
    # arn
    assert from_db.db_subnet_group_arn == f'arn:aws:rds:us-west-2:123456789012:subgrp:subnet_group_{index}'

    
    
@pytest.mark.parametrize("index", range(20))
def test_delete_many_from_prev_test(index):
    db_subnet_group_name=f'subnet_group_{index}'
    db_subnet_group_controller.delete_db_subnet_group(db_subnet_group_name)
    assert db_subnet_group_controller.get_db_subnet_group(db_subnet_group_name) == None
