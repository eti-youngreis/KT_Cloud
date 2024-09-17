import uuid
import os
import sys
import sqlite3
import json

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..','..')))

from Models.MultipartUploadModel import MultipartUploadModel
from Models.PartModel import PartModel
from DataAccess.MultipartUploadManager import MultipartUploadManager
from DataAccess.StorageManager import StorageManager

class MultipartUploadService:
    def __init__(self, db_file: str, storage_path: str):
        self.multipart_manager = MultipartUploadManager(db_file)
        self.storage_manager = StorageManager(storage_path)

    def initiate_multipart_upload(self, bucket_name: str, object_key: str) -> str:
        """
        יוזם העלאת קובץ מרובת חלקים ומחזיר את ה-upload_id.
        """
        multipart_upload = MultipartUploadModel(bucket_name=bucket_name, object_key=object_key,upload_id=str(uuid.uuid4()))
        upload_id = self.multipart_manager.create_multipart_upload(multipart_upload)
        return upload_id
    
    # פונקציה לפיצול קובץ לחלקים
    def split_file_into_parts(self,file_path: str, part_size: int = 5 * 1024 * 1024):
        """מפצל קובץ לחלקים לפי גודל מוגדר מראש"""
        parts = []
        with open(file_path, 'r') as file:
            part_number = 1
            while True:
                part_data = file.read(part_size)
                if not part_data:
                    break
                parts.append((part_number, part_data))
                part_number += 1
        return parts
    
    def upload_part(self,upload_id: str,part_number,part_data):
        criteria=f'object_id ="{upload_id}"'
        obj_part=self.multipart_manager.object_manager.get_from_memory(self.multipart_manager.object_name,criteria=criteria)
        multipart_upload =self.convert_to_object(obj_part)
        part_file_path = os.path.join(f'{multipart_upload.bucket_name}/part_{part_number}{multipart_upload.object_key}')
        part_model = PartModel(part_number=part_number,part_file_path=part_file_path, etag=f'etag_{part_number}', last_modified="2025-10-20")
        self.storage_manager.create_file(part_file_path,part_data)
        self.multipart_manager.upload_part(multipart_upload=multipart_upload, new_part=part_model, body=part_data)

    

    def upload_file_parts(self, upload_id: str, file_path: str, part_size: int = 1024):
        parts = self.split_file_into_parts(file_path, part_size)
        for part_number, part_data in parts:
            self.upload_part(upload_id=upload_id, part_number=part_number, part_data=part_data)

    def list_parts(self,upload_id):
        criteria=f'object_id ="{upload_id}"'
        obj=self.multipart_manager.object_manager.get_from_memory(self.multipart_manager.object_name,criteria=criteria)
        multipart_upload =self.convert_to_object(obj)
        return multipart_upload.parts

 
    
    def complete_upload(self, upload_id: str,bucket_name:str, object_key: str):
        criteria=f'object_id ="{upload_id}"'
        obj=self.multipart_manager.object_manager.get_from_memory(self.multipart_manager.object_name,criteria=criteria)
        multipart_upload =self.convert_to_object(obj)
        print("dddddddddd")
        if isinstance(multipart_upload.parts, str):
            multipart_upload.parts = json.loads(multipart_upload.parts)

        complete_file_path = os.path.join(self.storage_manager.base_directory, f'{bucket_name}/complete_{multipart_upload.object_key}')

        with open(complete_file_path, 'w') as complete_file:
            for part in sorted(multipart_upload.parts, key=lambda x: int(x['PartNumber'])):
                part_file_path = complete_file_path = os.path.join(self.storage_manager.base_directory,f"{part['FilePath']}")
                with open(part_file_path, 'r') as part_file:
                    complete_file.write(part_file.read())
        for part in multipart_upload.parts:
            os.remove(os.path.join(self.storage_manager.base_directory,f"{part['FilePath']}"))
        self.multipart_manager.complete_multipart_upload(multipart_upload)
        print(complete_file_path,"dddddddddd")
        return complete_file_path
    

    def convert_to_object(self, obj_dict: dict) -> MultipartUploadModel:
        """
        ממיר אובייקט מהטבלה בצורת מילון לאובייקט מסוג MultipartUploadModel.
        """
        print(obj_dict[0],"ooooooooooooooo")
        multipart_upload = MultipartUploadModel(
            bucket_name=obj_dict[0][2],
            object_key=obj_dict[0][1],
            upload_id=obj_dict[0][0],
            parts=obj_dict[0][3]
        )
        return multipart_upload

    
    def select_all_from_table(self,db_file: str, table_name: str):
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
