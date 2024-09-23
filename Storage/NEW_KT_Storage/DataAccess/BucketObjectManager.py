from Storage.NEW_KT_Storage.DataAccess.ObjectManager import ObjectManager
from Storage.NEW_KT_Storage.Models.BucketObjectModel import BucketObject
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..','..','..')))

class BucketObjectManager:

    def __init__(self, db_file: str):
        '''Initialize ObjectManager with the database connection.'''
        # create the path of db
        db_file += "\\" + "object.db"
        self.object_manager = ObjectManager(db_file)
        table_structure = ",".join(BucketObject.table_structure)
        self.object_manager.object_manager.create_management_table(BucketObject.object_name,table_structure)

    def createInMemoryBucketObject(self, bucket_object: BucketObject):
        self.object_manager.save_in_memory(BucketObject.object_name, bucket_object.to_sql())

    def deleteInMemoryBucketObject(self, bucket_name, object_name):
        obj_to_delete = BucketObject(bucket_name=bucket_name, object_key=object_name)
        obj_id = bucket_name + object_name
        self.object_manager.delete_from_memory_by_pk(BucketObject.object_name, BucketObject.pk_column, obj_id)

    def describeBucketObject(self, object_id):
        return self.object_manager.get_from_memory(BucketObject.object_name, criteria=f"{BucketObject.pk_column} = '{object_id}'")

    def getAllObjects(self, bucket_name):
        return self.object_manager.get_from_memory(BucketObject.object_name, criteria=f"bucket_id = '{bucket_name}'")
