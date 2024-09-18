import pytest
import sqlite3
import sys
import os

# Add paths to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))

from Service.Classes.DBClusterService import DBClusterService
from DataAccess.DBClusterManager import DBClusterManager
from Controller.DBClusterController import DBClusterController
from Storage.KT_Storage.DataAccess.StorageManager import StorageManager
from DataAccess.ObjectManager import ObjectManager
from Models.DBClusterModel import Cluster

@pytest.fixture(scope = 'module',autouse=True)
def setup_services():
   
    # Initialize the managers, services, controllers, and storage managers
    manager = DBClusterManager('Clusters/clusters.db')
    service = DBClusterService(manager,storage_manager, 'Clusters')
    controller = DBClusterController(service)
    storage_manager = StorageManager('Clusters')
    
    # Provide the variables as a dictionary or as individual items
    return {
        'manager': manager,
        'service': service,
        'controller': controller,
        'storage_manager': storage_manager
    }

# def test_create_DBCluster_function():
#     cluster_data = {
#     'db_cluster_identifier': 'myCluster5',
#     'engine': 'mysql',
#     'allocated_storage':5,
#     'db_subnet_group_name': 'my-subnet-group'
#     }

    # assert setup_services['controller'].create_db_cluster(**cluster_data)