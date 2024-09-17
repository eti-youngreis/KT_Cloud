from Service.Classes import DBProxyService


class DBProxyController:
    def __init__(self, service: DBProxyService) -> None:
        self.service = service

    def create_db_proxy(self,**kwargs):
        self.service.create(**kwargs)


    def delete_db_proxy(self, id):
        self.service.delete(id)

    def modify_db_proxy(self, id, **updates):
        self.service.modify(id, **updates)

        
