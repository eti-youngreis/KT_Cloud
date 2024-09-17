from DB.NEW_KT_DB.DataAccess.ObjectManager import ObjectManager as ObjectManagerDB
from Storage.NEW_KT_Storage.DataAccess.StorageManager import StorageManager

class ObjectManager:


    def __init__(self, db_file: str,type, storage_path="D:\\s3_project\\server"):
        """Initialize ObjectManager with the database connection."""
        self.object_manager = ObjectManagerDB(db_file)


    # for outer use:
    def save_in_memory(self, object_name, object_info, columns=None):

        self.object_manager.save_in_memory(object_name, object_info, columns)


    def delete_from_memory_by_criteria(self, object_name:str, criteria:str):

        self.object_manager.delete_from_memory_by_criteria(object_name, criteria)


    def delete_from_memory_by_pk(self, object_name:str, pk_column:str, pk_value:str):

        self.object_manager.delete_from_memory_by_pk(object_name, pk_column, pk_value)


    def update_in_memory(self, object_name, updates, criteria):

        self.object_manager.update_in_memory(object_name, updates, criteria)


    def get_from_memory(self, object_name, columns=None, criteria=None):

        if columns is None and criteria is None:
            return self.object_manager.get_from_memory(object_name)
        elif columns is None:
            return self.object_manager.get_from_memory(object_name, criteria=criteria)
        elif criteria is None:
            return self.object_manager.get_from_memory(object_name, columns)
        else:
            return self.object_manager.get_from_memory(object_name, columns, criteria)


    def convert_object_attributes_to_dictionary(self, **kwargs):

        return self.object_manager.convert_object_attributes_to_dictionary(**kwargs)


    def get_all_objects_from_memory(self, object_name):

        return self.object_manager.get_all_objects_from_memory(object_name)

