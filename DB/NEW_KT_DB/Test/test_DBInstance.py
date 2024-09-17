import os
import sys
import pytest
import sqlite3
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))
from Service.Classes.DBInstanceService import DBInstanceManager,DBInstanceService,AlreadyExistsError,ParamValidationError,DBInstanceNotFoundError
from Exception.exception import MissingRequireParamError
from Controller.DBInstanceController import DBInstanceController
from DataAccess.ObjectManager import ObjectManager

@pytest.fixture
def object_manager():
    return ObjectManager(':memory:')

@pytest.fixture
def db_instance_manager(object_manager):
    return DBInstanceManager(object_manager)

@pytest.fixture
def db_instance_service(db_instance_manager):
    return DBInstanceService(db_instance_manager)

@pytest.fixture
def snapshot_service(object_manager):
    return SnapShotService(SnapShotManager(object_manager))

@pytest.fixture
def db_instance_controller(db_instance_service):
    return DBInstanceController(db_instance_service)

def test_create_invalid_identifier(db_instance_controller):
    # Test for invalid db_instance_identifier
    with pytest.raises(ValueError):
        db_instance_controller.create_db_instance(
            db_instance_identifier="invalid!@#", 
            master_username="admin", 
            master_user_password="password"
        )

def test_create_missing_required_param(db_instance_controller):
    # Test for missing required parameter
    with pytest.raises(MissingRequireParamError):
        db_instance_controller.create_db_instance(
            master_username="admin", 
            master_user_password="password"
        )

def test_create_valid_db_instance(db_instance_controller):
    attributes = {
        "db_instance_identifier": "db123",
        "master_username": "admin",
        "master_user_password": "password"
    }
    response = db_instance_controller.create_db_instance(**attributes)
    assert response['DBInstance']['db_instance_identifier'] == "db123"
    assert response['DBInstance']['master_username'] == "admin"
    with pytest.raises(AlreadyExistsError):
         db_instance_controller.create_db_instance(**attributes)

def test_delete_db_instance(db_instance_controller):
    attributes = {
        "db_instance_identifier": "db123",
        "master_username": "admin",
        "master_user_password": "password"
    }
    
    db_instance_controller.create_db_instance(**attributes)
    print('db_instance_controller.describe_db_instance("db123"):',db_instance_controller.describe_db_instance("db123"))
    
    db_instance_controller.delete_db_instance({
        "db_instance_identifier": "db123",
        "skip_final_snapshot": True
    })
    with pytest.raises(DBInstanceNotFoundError):
        db_instance_controller.describe_db_instance("db123")

def test_delete_with_snapshot_invalide_params_db_instance(db_instance_controller,snapshot_service):
    attributes = {
        "db_instance_identifier": "db123",
        "master_username": "admin",
        "master_user_password": "password"
    }
    
    db_instance_controller.create_db_instance(**attributes)
    
    with pytest.raises(ParamValidationError):
        db_instance_controller.delete_db_instance({
            "db_instance_identifier": "db123",
            "skip_final_snapshot": False
        })

    with pytest.raises(DBInstanceNotFoundError):
        db_instance_controller.delete_db_instance({
            "db_instance_identifier": "invalide_id",
            "skip_final_snapshot": True
        })
    
    db_instance_controller.delete_db_instance({
        "db_instance_identifier": "db123",
        "skip_final_snapshot": False,
        "final_db_snapshot_identifier":"final_db_snapshot_identifier_db123"
    })

    snapshot_service.describe_db_instance("final_db_snapshot_identifier_db123")   

def test_modify_db_instance(db_instance_controller):
    attributes = {
        "db_instance_identifier": "db123",
        "master_username": "admin",
        "master_user_password": "password"
    }
    
    db_instance_controller.create_db_instance(**attributes)
    
    updates = {
        "db_instance_identifier": "db123",
        "allocated_storage": 50
    }
    
    response = db_instance_controller.modify_db_instance(**updates)
    
    assert response['DBInstance'].allocated_storage == 50

def test_describe_db_instance_not_found(db_instance_controller):
    with pytest.raises(DBInstanceNotFoundError):
        db_instance_controller.describe_db_instance("non_existent_instance")
