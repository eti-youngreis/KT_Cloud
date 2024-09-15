from Service import DBClusterService

class DBClusterController:
    def __init__(self, service: DBClusterService):
        self.service = service


    def create_db_cluster(self, **kwargs):
        self.service.create(**kwargs)


    def delete_db_cluster(self):
        self.service.delete()


    def modify_db_cluster(self, updates):
        self.service.modify(updates)
    