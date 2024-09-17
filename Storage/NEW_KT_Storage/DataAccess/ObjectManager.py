from DB.NEW_KT_DB.DataAccess.ObjectManager import ObjectManager as ObjectManagerDB
from Storage.NEW_KT_Storage.DataAccess.StorageManager import StorageManager

class ObjectManager:


    def __init__(self, db_file: str,type, storage_path="D:\\s3_project\\server"):
        """Initialize ObjectManager with the database connection."""
        self.object_manager = ObjectManagerDB(db_file)
        self.object_name = type

    # for outer use:
    def save_in_memory(self, object):

        # insert object info into management table mng_{object_name}s
        # for exmple: object db_instance will be saved in table mng_db_instances
        table_name = self.object_manager.convert_object_name_to_management_table_name(self.object_name)
        if not self.object_manager.is_management_table_exist(table_name):
            self.object_manager.create_management_table(table_name)

        self.object_manager.insert_object_to_management_table(table_name, object)


    def delete_from_memory(self,pk_column, pk_value, object_id, criteria='default'):

        # if criteria not sent- use PK for deletion
        if criteria == 'default':
            criteria = f"{pk_column} = '{pk_value}'"

        self.object_manager.delete_from_memory(self.object_name, criteria,object_id=object_id)


    def update_in_memory(self,object, updates, criteria='default'):

        # if criteria not sent- use PK for deletion
        if criteria == 'default':
            criteria = f'{object.pk_column} = {object.pk_value}'

        table_name = self.object_manager.convert_object_name_to_management_table_name(self.object_name)

        self.object_manager.update_object_in_management_table_by_criteria(table_name, updates, criteria)

    def get_from_memory(self, object_id):
        table_name = self.object_manager.convert_object_name_to_management_table_name(self.object_name)
        return self.object_manager.get_object_from_management_table(table_name, object_id)


    def get_data_from_memory_db(self):
        table_name = self.object_manager.convert_object_name_to_management_table_name(self.object_name)
        data_tuple = self.object_manager.get_all_data_from_table(table_name=table_name)
        data_list = [list(row) for row in data_tuple]
        return data_list


    def convert_object_attributes_to_dictionary(self, **kwargs):

        dict = {}

        for key, value in kwargs.items():
            dict[key] = value

        return dict

    def get_all_from_memory(self,criteria):
        table_name = self.object_manager.convert_object_name_to_management_table_name(self.object_name)
        return self.object_manager.get_objects_from_management_table_by_criteria(table_name, columns=["*"], criteria=criteria)

