# from Service.Classes import DBClusterService
from Service.Classes.DBClusterService import DBClusterService
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
    clusterController = DBClusterController()
    cluster_data = {
    'db_cluster_identifier': 'my-cluster-1',
    'engine': 'aurora-mysql',
    'db_subnet_group_name': 'my-subnet-group'
    }

    aaa = clusterController.create_db_cluster(**cluster_data)
    print(aaa)