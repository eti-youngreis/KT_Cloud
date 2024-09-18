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
from Service.Classes.BucketObjectService import BucketObjectService
from Validation.MultiPartValidations import validate_part_size,validate_upload_id

class MultipartUploadService:
    def __init__(self, db_file: str, storage_path: str):
        # Initializes the service with database and storage paths.
        self.multipart_manager = MultipartUploadManager(db_file)
        self.storage_manager = StorageManager(storage_path)
        self.bucketObjectService = BucketObjectService(storage_path)

    def initiate_multipart_upload(self, bucket_name: str, object_key: str) -> str:
        # Initiates a multipart upload and returns the unique upload ID.
        multipart_upload = MultipartUploadModel(bucket_name=bucket_name, object_key=object_key, upload_id=str(uuid.uuid4()))
        upload_id = self.multipart_manager.create_multipart_upload(multipart_upload)
        return upload_id

    def split_file_into_parts(self, file_path: str, part_size: int = 1024):
        # Splits a file into parts based on the specified size and returns a list of parts.
        if not self.storage_manager.is_file_exist(file_path):
            raise FileNotFoundError(f"The file at path {file_path} does not exist.")
        validate_part_size(part_size)
        parts = []
        part_number = 1
        offset = 0  # Track the current position in the file
        while True:
            part_data = self.storage_manager.get_content_file(file_path, part_size=part_size, offset=offset)
            if not part_data:
                break
            parts.append((part_number, part_data))
            part_number += 1
            offset += part_size  # Update the offset for the next part
        return parts

    def upload_part(self, upload_id: str, part_number, part_data):
        # Uploads a part of the file to the storage and associates it with the specified upload ID.
        criteria = f'object_id ="{upload_id}"'
        obj_part = self.multipart_manager.object_manager.get_from_memory(self.multipart_manager.object_name, criteria=criteria)
        multipart_upload = self.convert_to_object(obj_part)
        part_file_path = os.path.join(f'{multipart_upload.bucket_name}/part_{part_number}{multipart_upload.object_key}')
        part_model = PartModel(part_number=part_number, part_file_path=part_file_path, etag=f'etag_{part_number}', last_modified="2025-10-20")
        self.storage_manager.create_file(part_file_path, part_data)
        self.multipart_manager.upload_part(multipart_upload=multipart_upload, new_part=part_model, body=part_data)

    def upload_file_parts(self, upload_id: str, file_path: str, part_size: int = 1024):
        # Uploads all parts of a file based on the specified upload ID.
        validate_upload_id(upload_id)
        if not self.storage_manager.is_file_exist(file_path):
            raise FileNotFoundError(f"The file at path {file_path} does not exist.")
        validate_part_size(part_size)
        parts = self.split_file_into_parts(file_path, part_size)
        for part_number, part_data in parts:
            self.upload_part(upload_id=upload_id, part_number=part_number, part_data=part_data)

    def list_parts(self, upload_id):
        # Retrieves and returns the list of parts associated with the specified upload ID.
        validate_upload_id(upload_id)
        criteria = f'object_id ="{upload_id}"'
        obj = self.multipart_manager.object_manager.get_from_memory(self.multipart_manager.object_name, criteria=criteria)
        multipart_upload = self.convert_to_object(obj)
        return multipart_upload.parts

    def complete_upload(self, upload_id: str, bucket_name: str = None, object_key: str = None):
        # Completes the multipart upload by combining all parts into a single file.
        validate_upload_id(upload_id)
        criteria = f'object_id ="{upload_id}"'
        obj = self.multipart_manager.object_manager.get_from_memory(self.multipart_manager.object_name, criteria=criteria)
        multipart_upload = self.convert_to_object(obj)
        if isinstance(multipart_upload.parts, str):
            multipart_upload.parts = json.loads(multipart_upload.parts)         
        complete_file_path = os.path.join(self.storage_manager.base_directory, f'{multipart_upload.bucket_name}/{multipart_upload.object_key}')
        self.bucketObjectService.create(bucket_name=multipart_upload.bucket_name, object_key=multipart_upload.object_key, content='')
        for part in sorted(multipart_upload.parts, key=lambda x: int(x['PartNumber'])):
            part_file_path = os.path.join(self.storage_manager.base_directory, f"{part['FilePath']}")
            content = self.storage_manager.get_content_file(part_file_path)
            self.storage_manager.write_to_file(complete_file_path, content, mode='a')
        for part in multipart_upload.parts:
            self.storage_manager.delete_file(os.path.join(self.storage_manager.base_directory, f"{part['FilePath']}"))
        self.multipart_manager.complete_multipart_upload(multipart_upload)
        return complete_file_path

    def convert_to_object(self, obj_dict: dict) -> MultipartUploadModel:
        # Converts a dictionary representation of multipart upload data into a MultipartUploadModel object.
        if not obj_dict or not obj_dict[0]:
            raise ValueError("No data found to convert to MultipartUploadModel.")
        multipart_upload = MultipartUploadModel(
            bucket_name=obj_dict[0][2],
            object_key=obj_dict[0][1],
            upload_id=obj_dict[0][0],
            parts=json.loads(obj_dict[0][3]) if isinstance(obj_dict[0][3], str) else obj_dict[0][3]
        )
        return multipart_upload

    def select_all_from_table(self, db_file: str, table_name: str):
        # Selects and returns all data from a specified table in the database.
        try:
            conn = sqlite3.connect(db_file)
            cursor = conn.cursor()
            query = f"SELECT * FROM {table_name}"
            cursor.execute(query)
            rows = cursor.fetchall()
            cursor.close()
            conn.close()
            return rows
        except sqlite3.Error as e:
            print(f"An error occurred: {e}")
            return None
