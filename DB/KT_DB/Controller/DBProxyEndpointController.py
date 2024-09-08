from Service import DBProxyService
class DBProxyEndpointController:
    def __init__(self, service: DBProxyService):
        self.service = service

    def create_db_proxy_endpoint(self,kwargs):
        self.service.create(**kwargs)
    
    def delete_db_proxy_endpoint(self,kwargs):
        self.service.delete(**kwargs)
    
    def describe_db_proxy_endpoints(self,kwargs):
        self.service.describe(**kwargs)
        
    def modify_db_proxy_endpoint(self, kwargs):
        self.service.modify(**kwargs)
        
