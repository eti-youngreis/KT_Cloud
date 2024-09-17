# from Service.Classes import DBClusterService
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../..')))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from KT_Cloud.Storage.NEW_KT_Storage.DataAccess import StorageManager
from Service.Classes.DBClusterService import DBClusterService
from DataAccess import DBClusterManager
class DBClusterController:
    def __init__(self, service: DBClusterService):
        self.service = service


    def create_db_cluster(self, **kwargs):
        self.service.create(**kwargs)


    def delete_db_cluster(self):
        self.service.delete()


    def modify_db_cluster(self, updates):
        self.service.modify(updates)


if __name__=='__main__':

    desktop_path = os.path.join(os.path.expanduser('~'), 'Desktop')
    cluster_directory = os.path.join(desktop_path, f'Clusters/clusters.db')
    base = os.path.join(desktop_path, f'Clusters')
    storage_manager = StorageManager.StorageManager('Clusters')
    clusterManager = DBClusterManager.DBClusterManager('Clusters/clusters.db')
    clusterService = DBClusterService(clusterManager,storage_manager, 'Clusters')
    clusterController = DBClusterController(clusterService)
    cluster_data = {
    'db_cluster_identifier': 'my-cluster-3',
    'engine': 'mysql',
    'allocated_storage':5,
    'db_subnet_group_name': 'my-subnet-group'
    }

    aaa = clusterController.create_db_cluster(**cluster_data)
    print(aaa)