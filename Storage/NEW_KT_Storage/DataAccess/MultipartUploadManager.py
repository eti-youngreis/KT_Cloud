import sqlite3
import uuid
import os
import sys
import json
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))) 
from Models.MultipartUploadModel import MultipartUploadModel 
"""
Imports the ObjectManager class from the KT_Cloud.DB.NEW_KT_DB.DataAccess module.

The ObjectManager class is responsible for managing the storage and retrieval of objects in the database.
"""
from DataAccess.ObjectManager import ObjectManager

class MultipartUploadManager:
    def __init__(self, db_file: str, storage_path: str = None):
        self.storage_path = storage_path
        self.object_manager = ObjectManager(db_file)
        self.db_manager = self.object_manager.object_manager
        self.create_table()

    def create_table(self):
        table_schema = 'upload_id TEXT PRIMARY KEY, bucket_name TEXT NOT NULL, object_name TEXT NOT NULL, parts TEXT NOT NULL'
        self.db_manager.create_management_table('MultipartUploadModel', table_schema)
   


    def create_multipart_upload(self, multipart_upload: MultipartUploadModel) -> str:
        """יוצר תהליך העלאת חלקים ומחזיר UploadId ייחודי"""
        if not isinstance(multipart_upload, MultipartUploadModel):
            raise TypeError('Expected an instance of MultipartUploadModel')
        self.object_manager.save_in_memory(multipart_upload)
        return multipart_upload.upload_id

    # def create_multipart_upload(self, multipart_upload: MultipartUploadModel) -> str:
    #     """יוצר תהליך העלאת חלקים ומחזיר UploadId ייחודי"""
    #     # multipart_upload.upload_id = str(uuid.uuid4())
    #     multipart_dict = multipart_upload.to_dict()
    #     multipart_dict['parts'] = json.dumps(multipart_dict['parts'])
    #     self.db_manager.insert_object_to_management_table(self.table_name, multipart_dict)
    #     return multipart_upload.upload_id
    # def upload_part(self, bucket_name: str, object_key: str, upload_id: str, new_part: PartModel) -> str:
    #     """מעלה חלק מסוים עבור bucket ואובייקט"""
    #     part_file_path = os.path.join(self.storage_path, f'{bucket_name}/{object_key}_part_{new_part.part_number}')
    #     self.object_manager.create_file(part_file_path, new_part.body)
        
    #     # עדכון אובייקט ההעלאה עם החלק החדש
    #     obj_parts = self.db_manager.get_object_from_management_table(upload_id)
    #     multipart_upload = MultipartUploadModel(obj_parts['bucket_name'], obj_parts['object_key'])
    #     multipart_upload.parts = obj_parts['parts']
    #     multipart_upload.parts.append({
    #         'PartNumber': new_part.part_number,
    #         'FilePath': part_file_path
    #     })
        
    #     # עדכון במסד הנתונים
    #     self.db_manager.update_object_in_management_table_by_criteria(self.table_name, multipart_upload, upload_id)
        
    #     # Generate a fake ETag for the example
    #     return f'etag_{new_part.part_number}'

    # def complete_multipart_upload(self, bucket_name: str, object_key: str, upload_id: str):
    #     """משלים את ההעלאה, מאחד את כל החלקים לקובץ אחד"""
    #     obj_parts = self.db_manager.get_object_from_management_table(upload_id)
    #     complete_file_path = os.path.join(self.storage_path, f'{bucket_name}/{object_key}_complete')

    #     with open(complete_file_path, 'wb') as complete_file:
    #         for part in sorted(obj_parts.parts, key=lambda x: x['PartNumber']):
    #             part_file_path = part['FilePath']
    #             with open(part_file_path, 'rb') as part_file:
    #                 complete_file.write(part_file.read())

    #     # setObject(bucket_name, key, complete_file_path)

    #     # מחיקת חלקי הקבצים
    #     for part in obj_parts.parts:
    #         os.remove(part['FilePath'])
    import sqlite3

def select_all_from_table(db_file: str, table_name: str):
    """Selects all data from a specified table in the database."""
    try:
        # יצירת חיבור למסד הנתונים
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()

        # הרצת שאילתת SELECT
        query = f"SELECT * FROM {table_name}"
        cursor.execute(query)

        # שליפת כל התוצאות
        rows = cursor.fetchall()

        # סגירת החיבור למסד הנתונים
        cursor.close()
        conn.close()

        return rows
    except sqlite3.Error as e:
        print(f"An error occurred: {e}")
        return None

MultipartUploadModel_new = MultipartUploadModel("my_bucket", "my_object")
print(MultipartUploadModel_new.__class__.__name__)

MultipartUploadManager_new = MultipartUploadManager("my_db.db", "my_storage_path")
MultipartUploadModel_new.upload_id = MultipartUploadManager_new.create_multipart_upload(MultipartUploadModel_new)
db_file = "my_db.db"
table_name = "MultipartUploadModel"
# select_and_return_records_from_table# קריאה לפונקציה
all_data = select_all_from_table(db_file, table_name)

# הדפסת התוצאות
print(all_data)