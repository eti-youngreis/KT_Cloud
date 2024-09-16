from Service.Classes import DBSubnetGroupService

class DBSubnetGroupController:
    def __init__(self, service: DBSubnetGroupService):
        self.service = service


    def create_db_subnet_group(self, **kwargs):
        self.service.create_db_subnet_group(**kwargs)


    def delete_db_subnet_group(self, name):
        self.service.delete_db_subnet_group(name)


    def modify_db_subnet_group(self, name, updates):
        self.service.modify_db_subnet_group(name, updates)
        
    def get_db_subnet_group(self, name):
        return self.service.get_db_subnet_group(name)
    
    def list_all_subnets(self):
        return self.service.list_all_subnets()
    
    def create_subnet(self, subnet_id: str):
        self.service.create_subnet(subnet_id)
        
    def delete_subnet(self, subnet_id: str):
        self.service.delete_subnet(subnet_id)