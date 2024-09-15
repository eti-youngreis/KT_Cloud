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
from DataAccess.DBManager import DBManager
from Models.DBSubnetGroupModel import DBSubnetGroup

db_manager = DBManager('../object_management_db.db')
object_manager = ObjectManager('../object_management_db.db')
manager = DBSubnetGroupManager(db_manager=db_manager, object_manager=object_manager)
service = DBSubnetGroupService(manager)
controller = DBSubnetGroupController(service)
storage_manager = StorageManager()
    
def test_create():
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

    # assert bucket was created
    # check that file was created
    assert storage_manager.get('db_subnet_groups', 'subnet_group_1', '0') != None
    # check that object was saved to management table
    assert controller.get_db_subnet_group('subnet_group_1') != None
    # check that file content is correct
    from_storage = DBSubnetGroup.from_bytes_to_dict(storage_manager.get('db_subnet_groups', 'subnet_group_1', '0')['content'])
    from_db = controller.get_db_subnet_group('subnet_group_1').to_dict()
    assert from_storage['db_subnet_group_name'] == from_db['db_subnet_group_name']
    for subnet in from_storage['subnets']:
        assert subnet in json.loads(from_db['subnets'])
    assert from_storage['db_subnet_group_description'] == from_db['db_subnet_group_description']
    assert from_storage['vpc_id'] == from_db['vpc_id']
    assert from_storage['db_subnet_group_arn'] == from_db['db_subnet_group_arn']
    assert from_storage['status'] == from_db['status']
        
def test_unique_constraint():
    with pytest.raises(IntegrityError, match="UNIQUE constraint failed"):
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
        
def test_modify():
    controller.modify_db_subnet_group(
        name='subnet_group_1',
        updates= {'subnets':[
            {'subnet_id': 'subnet-12345988'},
            {'subnet_id': 'subnet-876543881'}]}
    )
    
    from_storage = DBSubnetGroup.from_bytes_to_dict(storage_manager.get('db_subnet_groups', 'subnet_group_1', '0')['content'])
    from_db = controller.get_db_subnet_group('subnet_group_1')
    assert from_storage['db_subnet_group_name'] == from_db.db_subnet_group_name
    for subnet in from_storage['subnets']:
        assert str(subnet) in from_db.subnets
    assert from_storage['db_subnet_group_description'] == from_db.db_subnet_group_description
    assert from_storage['vpc_id'] == from_db.vpc_id
    assert from_storage['db_subnet_group_arn'] == from_db.db_subnet_group_arn
    assert from_storage['status'] == from_db.status

def test_get():
    subnet_group_1 = controller.get_db_subnet_group('subnet_group_1')
    assert subnet_group_1 != None
    assert subnet_group_1.db_subnet_group_name == 'subnet_group_1'
    assert subnet_group_1.db_subnet_group_description == 'Test subnet group'
    assert subnet_group_1.vpc_id == 'vpc-12345678'  
    assert subnet_group_1.db_subnet_group_arn == 'arn:aws:rds:us-west-2:123456789012:subgrp:subnet_group_1'
    assert subnet_group_1.status == 'pending'
    for subnet in json.loads(subnet_group_1.subnets):
        assert subnet in [
            {"subnet_id": "subnet-12345678"},
            {"subnet_id": "subnet-87654321"}
        ]
    

def test_describe():
    subnet_group_1 = controller.describe_db_subnet_group('subnet_group_1')
    assert subnet_group_1 != None
    assert subnet_group_1.db_subnet_group_name == 'subnet_group_1'
    assert subnet_group_1.db_subnet_group_description == 'Test subnet group'
    assert subnet_group_1.vpc_id == 'vpc-12345678'
    assert subnet_group_1.db_subnet_group_arn == 'arn:aws:rds:us-west-2:123456789012:subgrp:subnet_group_1'
    assert subnet_group_1.status == 'pending'
    for subnet in json.loads(subnet_group_1.subnets):
        assert subnet in json.dumps([
            {'subnet_id': 'subnet-12345678'},
            {'subnet_id': 'subnet-87654321'}
        ])
    
    
def test_delete():
    controller.delete_db_subnet_group('subnet_group_1')
    assert storage_manager.get('db_subnet_groups', 'subnet_group_1', '0') == None
    assert controller.get_db_subnet_group('subnet_group_1') == None
    
    
    