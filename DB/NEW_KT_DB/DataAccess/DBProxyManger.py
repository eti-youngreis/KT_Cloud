from DataAccess import ObjectManager


class DBProxyManger:
    def __init__(self, db_file: str) -> None:
        self.object_manger = ObjectManager(db_file)


    def create_in_memory_DBproxy(self, object_name, metadata, object_id):
        self.object_manger.save_in_memory(object_name, metadata, object_id)


    def delete_in_memory_DBProxy(self,object_name,object_id):
        self.object_manager.delete_from_memory(object_name=object_name,object_id=object_id)


    def describe_DBProxy(self, object_name, object_id):
        return self.object_manager.get_from_memory(object_name=object_name, object_id=object_id)


    def modify_DBProxy(self, object_name, updates, db_proxy_name):
        self.object_manager.update_in_memory(object_name=object_name, updates=updates, db_proxy_name=db_proxy_name) 


