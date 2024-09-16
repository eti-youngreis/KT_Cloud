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
    
    def describe_db_subnet_group(self, name):
        return self.service.describe_db_subnet_group(name)
    