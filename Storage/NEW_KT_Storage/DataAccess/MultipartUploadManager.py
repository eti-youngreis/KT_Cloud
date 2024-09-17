import sqlite3
import uuid
import os
import sys
import json

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from Models.MultipartUploadModel import MultipartUploadModel
from Models.PartModel import PartModel

"""
Imports the ObjectManager class from the KT_Cloud.DB.NEW_KT_DB.DataAccess module.
The ObjectManager class is responsible for managing the storage and retrieval of objects in the database.
"""
from DataAccess.ObjectManager import ObjectManager

class MultipartUploadManager:
    def __init__(self, db_file: str):
        self.object_manager = ObjectManager(db_file=db_file)
        self.object_name="MultipartUpload"
        self.create_table()


    def create_table(self):
        table_columns = (
            "object_id TEXT PRIMARY KEY",
            "object_name TEXT NOT NULL",
            "bucket_name TEXT NOT NULL",
            "parts TEXT NOT NULL"
        )
        columns_str = ", ".join(table_columns)
        print(columns_str,"print(columns_str)")

        self.object_manager.object_manager.create_management_table('MultipartUpload', columns_str)



    def create_multipart_upload(self, multipart_upload: MultipartUploadModel) -> str:
        """יוצר תהליך העלאת חלקים ומחזיר UploadId ייחודי"""
        if not isinstance(multipart_upload, MultipartUploadModel):
            raise TypeError('Expected an instance of MultipartUploadModel')
        self.object_manager.save_in_memory(self.object_name,multipart_upload.to_sql())
        return multipart_upload.upload_id
    

    def upload_part(self, multipart_upload: MultipartUploadModel, new_part: PartModel,body:str) -> str:
        """מעלה חלק מסוים עבור bucket ואובייקט"""
        # print(f'{multipart_upload.bucket_name}/part_{new_part.part_number}{multipart_upload.object_key}',"part_file_path")
        # # part_file_path = os.path.join(f'{multipart_upload.bucket_name}/part_{new_part.part_number}{multipart_upload.object_key}')
        # # self.object_manager.storage_manager.create_file(part_file_path, body)

        if isinstance(multipart_upload.parts, str):
            multipart_upload.parts = json.loads(multipart_upload.parts)

        # עדכון אובייקט ההעלאה עם החלק החדש
        multipart_upload.parts.append({
            'PartNumber': new_part.part_number,
            'FilePath': new_part.part_file_path
        })
        # # עדכון במסד הנתונים
        str_parts=json.dumps(multipart_upload.parts)
        update_statement = f"parts = '{str_parts}'"
        criteria=f"object_id = '{multipart_upload.upload_id}'"
        self.object_manager.update_in_memory(self.object_name,updates=update_statement,criteria=criteria)
        # # Generate a fake ETag for the example
        return f'etag_{new_part.part_number}'
   




    def complete_multipart_upload(self, multipart_upload: MultipartUploadModel):
        self.object_manager.delete_from_memory_by_pk(self.object_name,multipart_upload.pk_column, multipart_upload.pk_value)






  


 
    

