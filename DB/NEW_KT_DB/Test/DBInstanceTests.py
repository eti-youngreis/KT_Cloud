import os
import sys
import pytest
import sqlite3
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))
from NEW_KT_DB.Service.Classes.DBInstanceService import DBInstanceManager,DBInstanceService
from NEW_KT_DB.Controller.DBInstanceController import DBInstanceController


@pytest.fixture
def db_instance_manager():
    return DBInstanceManager(':memory:')

@pytest.fixture
def db_instance_service(user_group_manager):
    return DBInstanceService(user_group_manager)

@pytest.fixture
def db_instance_controller(user_group_service):
    return DBInstanceController(user_group_service)
