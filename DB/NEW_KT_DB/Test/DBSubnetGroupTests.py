from sqlite3 import IntegrityError
import pytest
import json

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))

from Service.Classes.DBSubnetGroupService import DBSubnetGroupService
from DataAccess.DBSubnetGroupManager import DBSubnetGroupManager
from Controller.DBSubnetGroupController import DBSubnetGroupController
from Storage.KT_Storage.DataAccess.StorageManager import StorageManager
from DataAccess.ObjectManager import ObjectManager
from Models.DBSubnetGroupModel import DBSubnetGroup
import sqlite3

object_manager = ObjectManager('../object_management_db.db')
manager = DBSubnetGroupManager(object_manager=object_manager)
service = DBSubnetGroupService(manager)
controller = DBSubnetGroupController(service)
storage_manager = StorageManager()

@pytest.fixture
def clear_table():
    # Connect to the SQLite database
    conn = conn = sqlite3.connect('../object_management_db.db')
    cursor = conn.cursor()

    # Clear the table if it exists
    table_name = "db_subnet_groups"
    cursor.execute(f"DELETE FROM {table_name};")

    # Commit changes and close the connection
    conn.commit()
    conn.close()

    # Yield to allow tests to run
    yield
        
def test_create(clear_table):
    
    # remove existing subnet group from previous tests
    controller.create_db_subnet_group(
        db_subnet_group_name='subnet_group_1',
        subnets=[
            {'subnet_id': 'subnet-12345678'},
            {'subnet_id': 'subnet-87654321'}
        ],
        db_subnet_group_description='Test subnet group',
        vpc_id='vpc-12345678',
        db_subnet_group_arn='arn:aws:rds:us-west-2:123456789012:subgrp:subnet_group_1'
    )

    # check that file was created (no error raised on get)
    storage_manager.get('db_subnet_groups', 'subnet_group_1', '0')
    # check that object was saved to management table (no error raised on get)
    controller.get_db_subnet_group('subnet_group_1')
    
    # check that file content is correct
    from_storage = DBSubnetGroup(**DBSubnetGroup.from_bytes_to_dict(storage_manager.get('db_subnet_groups', 'subnet_group_1', '0')['content']))
    from_db = controller.get_db_subnet_group('subnet_group_1')
    # check values were stored correctly in management table as well as storage
    
    # group name
    assert from_storage.db_subnet_group_name == 'subnet_group_1'
    assert from_db.db_subnet_group_name == 'subnet_group_1'
    
    # subnets
    for subnet in from_storage.subnets:
        assert subnet in [
            {'subnet_id': 'subnet-12345678'},
            {'subnet_id': 'subnet-87654321'}
        ]
    for subnet in from_db.subnets:
        assert subnet in [
            {'subnet_id': 'subnet-12345678'},
            {'subnet_id': 'subnet-87654321'}
        ]
    
    # description
    assert from_storage.db_subnet_group_description == 'Test subnet group'
    assert from_db.db_subnet_group_description == 'Test subnet group'
    
    # vpc_id
    assert from_storage.vpc_id == 'vpc-12345678'
    assert from_db.vpc_id == 'vpc-12345678'
    
    # arn
    assert from_storage.db_subnet_group_arn == 'arn:aws:rds:us-west-2:123456789012:subgrp:subnet_group_1'
    assert from_db.db_subnet_group_arn == 'arn:aws:rds:us-west-2:123456789012:subgrp:subnet_group_1'

        
def test_unique_constraint():
    with pytest.raises(ValueError):
        controller.create_db_subnet_group(
            db_subnet_group_name='subnet_group_1', 
            subnets=[
                {'subnet_id': 'subnet-87654321'},
                {'subnet_id': 'subnet-12345678'}
            ],
            db_subnet_group_description='Another subnet group with same name',
            vpc_id='vpc-87654321',
            db_subnet_group_arn='arn:aws:rds:us-west-2:123456789012:subgrp:subnet_group_1'
        )


def test_get():
    subnet_group_1 = controller.get_db_subnet_group('subnet_group_1')
    assert subnet_group_1 != None
    assert subnet_group_1.db_subnet_group_name == 'subnet_group_1'
    assert subnet_group_1.db_subnet_group_description == 'Test subnet group'
    assert subnet_group_1.vpc_id == 'vpc-12345678'  
    assert subnet_group_1.db_subnet_group_arn == 'arn:aws:rds:us-west-2:123456789012:subgrp:subnet_group_1'
    assert subnet_group_1.status == 'pending'
    for subnet in subnet_group_1.subnets:
        assert subnet in [
            {"subnet_id": "subnet-12345678"},
            {"subnet_id": "subnet-87654321"}
        ]
    
def test_modify():
    controller.modify_db_subnet_group(
        name='subnet_group_1',
        updates= {'subnets':[
            {'subnet_id': 'subnet-12345988'},
            {'subnet_id': 'subnet-876543881'}]}
    )
    
    from_storage = DBSubnetGroup(**DBSubnetGroup.from_bytes_to_dict(storage_manager.get('db_subnet_groups', 'subnet_group_1', '0')['content']))
    from_db = controller.get_db_subnet_group('subnet_group_1')
    assert from_storage.db_subnet_group_name == from_db.db_subnet_group_name
    for subnet in from_storage.subnets:
        assert subnet in from_db.subnets
    assert from_storage.db_subnet_group_description == from_db.db_subnet_group_description
    assert from_storage.vpc_id == from_db.vpc_id
    assert from_storage.db_subnet_group_arn == from_db.db_subnet_group_arn
    assert from_storage.status == from_db.status
    
def test_describe():
    subnet_group_1 = controller.describe_db_subnet_group('subnet_group_1')
    assert type(subnet_group_1) == dict
    assert type(subnet_group_1['subnets']) == list
    assert type(subnet_group_1['subnets'][0]) == dict
    subnet_group_1 = DBSubnetGroup(**subnet_group_1)
    assert subnet_group_1 != None
    assert subnet_group_1.db_subnet_group_name == 'subnet_group_1'
    assert subnet_group_1.db_subnet_group_description == 'Test subnet group'
    assert subnet_group_1.vpc_id == 'vpc-12345678'
    assert subnet_group_1.db_subnet_group_arn == 'arn:aws:rds:us-west-2:123456789012:subgrp:subnet_group_1'
    assert subnet_group_1.status == 'pending'
    for subnet in subnet_group_1.subnets:
        assert subnet in [
            {'subnet_id': 'subnet-12345988'},
            {'subnet_id': 'subnet-876543881'}
        ]
    
    
def test_delete():
    controller.delete_db_subnet_group('subnet_group_1')
    with pytest.raises(FileNotFoundError):
        storage_manager.get('db_subnet_groups', 'subnet_group_1', '0')
    with pytest.raises(Exception):
        controller.get_db_subnet_group('subnet_group_1')
    

@pytest.mark.parametrize("index", range(20))
def test_insert_many(index):
    controller.create_db_subnet_group(
        db_subnet_group_name=f'subnet_group_{index}',
        subnets=[
            {'subnet_id': f'subnet-1234567{index}'},
            {'subnet_id': f'subnet-8765432{index}'}
        ],
        db_subnet_group_description=f'Test subnet group {index}',
        vpc_id=f'vpc-1234567{index}',
        db_subnet_group_arn=f'arn:aws:rds:us-west-2:123456789012:subgrp:subnet_group_{index}'
    )
    
    # check that file was created (no error raised on get)
    storage_manager.get('db_subnet_groups', f'subnet_group_{index}', '0')
    # check that object was saved to management table (no error raised on get)
    controller.get_db_subnet_group(f'subnet_group_{index}')
    
    # check that file content is correct
    from_storage = DBSubnetGroup(**DBSubnetGroup.from_bytes_to_dict(storage_manager.get('db_subnet_groups', f'subnet_group_{index}', '0')['content']))
    from_db = controller.get_db_subnet_group(f'subnet_group_{index}')
    # check values were stored correctly in management table as well as storage
    
    # group name
    assert from_storage.db_subnet_group_name == f'subnet_group_{index}'
    assert from_db.db_subnet_group_name == f'subnet_group_{index}'
    
    # subnets
    for subnet in from_storage.subnets:
        assert subnet in [
            {'subnet_id': f'subnet-1234567{index}'},
            {'subnet_id': f'subnet-8765432{index}'}
        ]
    for subnet in from_db.subnets:
        assert subnet in [
            {'subnet_id': f'subnet-1234567{index}'},
            {'subnet_id': f'subnet-8765432{index}'}
        ]
    
    # description
    assert from_storage.db_subnet_group_description == f'Test subnet group {index}'
    assert from_db.db_subnet_group_description == f'Test subnet group {index}'
    
    # vpc_id
    assert from_storage.vpc_id == f'vpc-1234567{index}'
    assert from_db.vpc_id == f'vpc-1234567{index}'
    
    # arn
    assert from_storage.db_subnet_group_arn == f'arn:aws:rds:us-west-2:123456789012:subgrp:subnet_group_{index}'
    assert from_db.db_subnet_group_arn == f'arn:aws:rds:us-west-2:123456789012:subgrp:subnet_group_{index}'

    
    
@pytest.mark.parametrize("index", range(20))
def test_delete_many_from_prev_test(index):
    db_subnet_group_name=f'subnet_group_{index}'
    controller.delete_db_subnet_group(db_subnet_group_name)
    with pytest.raises(FileNotFoundError):
        storage_manager.get('db_subnet_groups', db_subnet_group_name, '0')
    with pytest.raises(Exception):
        controller.get_db_subnet_group(db_subnet_group_name)
