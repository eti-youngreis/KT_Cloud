import uuid
import os
import sys
import sqlite3
import json

# Add the parent directories to the system path for module imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

# Import models and services for multipart upload handling
from Models.MultipartUploadModel import MultipartUploadModel
from Models.PartModel import PartModel
from DataAccess.MultipartUploadManager import MultipartUploadManager
from DataAccess.StorageManager import StorageManager
from Service.Classes.BucketObjectService import BucketObjectService
from Validation.MultiPartValidations import validate_part_size, validate_upload_id

class MultipartUploadService:
    def __init__(self,db_file:str="my_db.db", storage_path: str="C:/Users/shana/Desktop/a"):
        # Initializes the service with database and storage paths for managing multipart uploads
        self.multipart_manager = MultipartUploadManager(db_file)
        self.storage_manager = StorageManager(storage_path)
        self.bucketObjectService = BucketObjectService(storage_path)

    def initiate_multipart_upload(self, bucket_name: str, object_key: str) -> str:
        # Initiates a multipart upload for a given bucket and object, and returns a unique upload ID
        multipart_upload = MultipartUploadModel(bucket_name=bucket_name, object_key=object_key, upload_id=str(uuid.uuid4()))
        upload_id = self.multipart_manager.create_multipart_upload(multipart_upload)
        return upload_id

    def split_file_into_parts(self, file_path: str, part_size: int = 1024):
        # Splits a file into multiple parts based on the specified part size and returns the parts
        if not self.storage_manager.is_file_exist(file_path):
            raise FileNotFoundError(f"The file at path {file_path} does not exist.")
        
        # Validate the part size for the multipart upload
        validate_part_size(part_size)
        
        parts = []
        part_number = 1
        offset = 0  # Track the current file read position
        
        while True:
            # Read part of the file content based on the part size and offset
            part_data = self.storage_manager.get_file_content(file_path, part_size=part_size, offset=offset)
            if not part_data:
                break  # Stop if no more data is available
            parts.append((part_number, part_data))
            part_number += 1
            offset += part_size  # Move the offset for the next part
            
        return parts

    def upload_part(self, upload_id: str, part_number, part_data):
        # Uploads a part to storage and associates it with a multipart upload ID
        multipart_upload = self._get_multipart_upload(upload_id)
        part_file_path = os.path.join(f'{multipart_upload.bucket_name}/part_{part_number}{multipart_upload.object_key}')
        
        # Create a model for the uploaded part and save it
        part_model = PartModel(part_number=part_number, part_file_path=part_file_path, etag=f'etag_{part_number}', last_modified="2025-10-20")
        self.storage_manager.create_file(part_file_path, part_data)
        self.multipart_manager.upload_part(multipart_upload=multipart_upload, new_part=part_model)

    def upload_file_parts(self, upload_id: str, file_path: str, part_size: int = 1024):
        # Uploads all parts of a file for a given upload ID
        validate_upload_id(upload_id)
        
        # Check if the file exists and validate the part size
        if not self.storage_manager.is_file_exist(file_path):
            raise FileNotFoundError(f"The file at path {file_path} does not exist.")
        
        validate_part_size(part_size)
        
        # Split the file into parts and upload each part
        parts = self.split_file_into_parts(file_path, part_size)
        for part_number, part_data in parts:
            self.upload_part(upload_id=upload_id, part_number=part_number, part_data=part_data)

    def list_parts(self, upload_id):
        # Lists all the parts associated with a specific multipart upload ID
        validate_upload_id(upload_id)
        multipart_upload = self._get_multipart_upload(upload_id)
        return multipart_upload.parts

    def abort_multipart_upload(self, upload_id: str):
        # Aborts the multipart upload, deletes all uploaded parts, and removes the upload record
        validate_upload_id(upload_id)
        multipart_upload = self._get_multipart_upload(upload_id)
        if multipart_upload.parts==[]:
            raise ValueError("No parts found for this upload.")
        self._delete_parts(multipart_upload)
        self.multipart_manager.delete_multipart_upload(multipart_upload)
        return f"Upload with ID {upload_id} has been aborted successfully."

    def complete_upload(self, upload_id: str, bucket_name: str = None, object_key: str = None):
        # Completes the multipart upload by combining all parts into a single file
        validate_upload_id(upload_id)
        multipart_upload = self._get_multipart_upload(upload_id)
        
        # Generate the complete file path and create an empty file for the final object
        complete_file_path = os.path.join(self.storage_manager.base_directory, f'{multipart_upload.bucket_name}/{multipart_upload.object_key}')
        self.bucketObjectService.create(bucket_name=multipart_upload.bucket_name, object_key=multipart_upload.object_key, content='')
        
        # Combine all parts and delete them after merging
        self._combine_parts(multipart_upload, complete_file_path)
        self._delete_parts(multipart_upload)
        self.multipart_manager.delete_multipart_upload(multipart_upload)
        return complete_file_path

    def _get_multipart_upload(self, upload_id: str):
        # Retrieves the multipart upload object from the database using the upload ID
        validate_upload_id(upload_id)
        criteria = f'object_id ="{upload_id}"'
        obj = self.multipart_manager.object_manager.get_from_memory(self.multipart_manager.object_name, criteria=criteria)
        multipart_upload = self.convert_to_object(obj)
        
        # Convert the parts to a list if needed
        if isinstance(multipart_upload.parts, str):
            multipart_upload.parts = json.loads(multipart_upload.parts)        
        return multipart_upload

    def _delete_parts(self, multipart_upload):
        # Deletes all parts of a multipart upload from the storage
        for part in multipart_upload.parts:
            part_file_path = os.path.join(self.storage_manager.base_directory, f"{part['FilePath']}")
            if self.storage_manager.is_file_exist(part_file_path):
                self.storage_manager.delete_file(part_file_path)

    def _combine_parts(self, multipart_upload, complete_file_path):
        # Combines all parts of a multipart upload into a single file in the correct order
        for part in sorted(multipart_upload.parts, key=lambda x: int(x['PartNumber'])):
            part_file_path = os.path.join(self.storage_manager.base_directory, f"{part['FilePath']}")
            content = self.storage_manager.get_file_content(part_file_path)
            self.storage_manager.write_to_file(complete_file_path, content, mode='a')

    def convert_to_object(self, obj_dict: dict) -> MultipartUploadModel:
        # Converts a dictionary representing multipart upload data into a MultipartUploadModel object
        if not obj_dict or not obj_dict[0]:
            raise ValueError("No data found to convert to MultipartUploadModel.")
        
        multipart_upload = MultipartUploadModel(
            bucket_name=obj_dict[0][2],
            object_key=obj_dict[0][1],
            upload_id=obj_dict[0][0],
            parts=json.loads(obj_dict[0][3]) if isinstance(obj_dict[0][3], str) else obj_dict[0][3]
        )
        return multipart_upload



