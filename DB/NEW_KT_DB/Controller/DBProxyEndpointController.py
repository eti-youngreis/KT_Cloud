from Service.Classes.DBProxyEndpointService import DBProxyEndpointService

class DBProxyEndpointController:
    def __init__(self, service: DBProxyEndpointService):
        self.service = service


    def create_db_proxy_endpoint(self, **kwargs):
        self.service.create(**kwargs)


    def delete_db_proxy_endpoint(self):
        self.service.delete()


    def modify_db_proxy_endpoint(self, updates):
        self.service.modify(updates)
    
    
    def describe_db_proxy_endpoint(self):
        self.service.describe()
    