# from Service.Classes import DBClusterService
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../..')))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from KT_Cloud.Storage.NEW_KT_Storage.DataAccess import StorageManager
from Service.Classes.DBClusterService import DBClusterService
from DataAccess import DBClusterManager
from Controller import DBInstanceController
from Service.Classes.DBInstanceService import DBInstanceService
from DataAccess import DBInstanceManager
from DataAccess import ObjectManager
class DBClusterController:
    def __init__(self, service: DBClusterService, instance_controller:DBInstanceController):
        self.service = service
        self.instance_controller = instance_controller


    def create_db_cluster(self, **kwargs):
        self.service.create(self.instance_controller,**kwargs)


    def delete_db_cluster(self , cluster_identifier):
        self.service.delete(cluster_identifier)


    def modify_db_cluster(self, cluster_identifier, **updates):
        self.service.modify(cluster_identifier,**updates)

    def describe_db_cluster(self, cluster_id):
        return self.service.describe(cluster_id)

if __name__=='__main__':

    # desktop_path = os.path.join(os.path.expanduser('~'), 'Desktop')
    # cluster_directory = os.path.join(desktop_path, f'Clusters/clusters.db')
    # base = os.path.join(desktop_path, f'Clusters')
    storage_manager = StorageManager.StorageManager('Instances')
    db_file = ObjectManager.ObjectManager('Clusters/instances.db')
    instance_manager = DBInstanceManager.DBInstanceManager(db_file)
    instanceService = DBInstanceService(instance_manager)
    instanceController = DBInstanceController.DBInstanceController(instanceService)

    storage_manager = StorageManager.StorageManager('Clusters')
    clusterManager = DBClusterManager.DBClusterManager('Clusters/clusters.db')
    clusterService = DBClusterService(clusterManager,storage_manager, 'Clusters')
    clusterController = DBClusterController(clusterService,instanceController)
    cluster_data = {
    'db_cluster_identifier': 'myCluster6',
    'engine': 'mysql',
    'allocated_storage':5,
    'db_subnet_group_name': 'my-subnet-group'
    }

    clusterController.create_db_cluster(**cluster_data)
    # clusterController.delete_db_cluster('myCluster5')
    update_data = {
    'engine': 'postgres',
    'allocated_storage':3,
    }
    # clusterController.modify_db_cluster('myCluster1', **update_data)
    
    # dfgh = clusterController.describe_db_cluster('myCluster3')
    # print(dfgh)



