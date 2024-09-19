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
        self.service.delete(self.instance_controller, cluster_identifier)


    def modify_db_cluster(self, cluster_identifier, **updates):
        self.service.modify(cluster_identifier,**updates)

    def describe_db_cluster(self, cluster_id):
        return self.service.describe(cluster_id)

    def get_all_db_clusters(self):
        return self.service.get_all_cluster()
    


