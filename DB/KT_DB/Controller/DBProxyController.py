from Service import DBProxyService
class DBProxyController:
    def __init__(self, service: DBProxyService):
        self.service = service

    def create_db_proxy(self,kwargs):
        """create a db proxy"""
        self.service.create(**kwargs)
    
    def delete_db_proxy(self,kwargs):
        """delete a db proxy"""
        self.service.delete(**kwargs)
    
    def describe_db_proxies(self,kwargs):
        """describe db proxies"""
        self.service.describe(**kwargs)
        
    def modify_db_proxy(self, kwargs):
        """modify a db proxy"""
        self.service.modify(**kwargs)
        
