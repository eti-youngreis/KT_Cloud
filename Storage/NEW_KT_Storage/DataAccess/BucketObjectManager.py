from typing import Dict, Any
import json
import sqlite3
from Storage.NEW_KT_Storage.DataAccess.ObjectManager import ObjectManager
from Storage.NEW_KT_Storage.Models.BucketObjectModel import BucketObject



class BucketObjectManager:


    def __init__(self, db_file: str=None):
        '''Initialize ObjectManager with the database connection.'''
        if db_file is None:
            db_file="C:\\Users\\user1\\Desktop\\server\\object.db"
        self.object_manager = ObjectManager(db_file,"Objects")
        self.create_table()

    def create_table(self):
        table_columns = "object_id TEXT PRIMARY KEY","bucket_id TEXT","object_key TEXT","encryption_id INTEGER","lock_id INTEGER"
        columns_str = ",".join(table_columns)
        self.object_manager.object_manager.db_manager.create_table("mng_Objectss",columns_str)


    def createInMemoryBucketObject(self, bucket_object: BucketObject):
        self.object_manager.save_in_memory(bucket_object.to_sql())


    def deleteInMemoryBucketObject(self, bucket_name, object_name):
        obj_to_delete=BucketObject(bucket_name=bucket_name,object_key=object_name)
        obj_id=bucket_name+ object_name
        self.object_manager.delete_from_memory("object_id",obj_id,obj_id)


    def describeBucketObject(self,object_id):
        return self.object_manager.get_from_memory(object_id)


    def putBucketObject(self,bucket_object: BucketObject,**updates):
        # add code to extract all data from self and send it as new updates
        updates = ''
        self.object_manager.update_in_memory(bucket_object,updates)

    def getAllObjects(self,bucket_name):
        return self.object_manager.get_all_from_memory(f"bucket_id = '{bucket_name}'")


    
