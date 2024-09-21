from Storage.NEW_KT_Storage.DataAccess.ObjectManager import ObjectManager
from Storage.NEW_KT_Storage.Models.BucketObjectModel import BucketObject


class BucketObjectManager:

    def __init__(self, db_file: str):
        '''Initialize ObjectManager with the database connection.'''
        db_file += "\\" + "object.db"
        self.object_manager = ObjectManager(db_file)
        self.object_name = "Object"
        self.pk_column = "object_id"
        self.create_table()


    def create_table(self):
        table_columns = "object_id TEXT PRIMARY KEY", "bucket_id TEXT", "object_key TEXT", "encryption_id INTEGER", "lock_id INTEGER"
        table_structure = ",".join(table_columns)
        self.object_manager.object_manager.create_management_table(self.object_name, table_structure)

    def createInMemoryBucketObject(self, bucket_object: BucketObject):
        self.object_manager.save_in_memory(self.object_name, bucket_object.to_sql())

    def deleteInMemoryBucketObject(self, bucket_name, object_name):
        obj_to_delete = BucketObject(bucket_name=bucket_name, object_key=object_name)
        obj_id = bucket_name + object_name
        self.object_manager.delete_from_memory_by_pk(self.object_name, self.pk_column, obj_id)

    def describeBucketObject(self, object_id):
        return self.object_manager.get_from_memory(self.object_name, criteria=f"{self.pk_column} = '{object_id}'")

    def getAllObjects(self, bucket_name):
        return self.object_manager.get_from_memory(self.object_name, criteria=f"bucket_id = '{bucket_name}'")
