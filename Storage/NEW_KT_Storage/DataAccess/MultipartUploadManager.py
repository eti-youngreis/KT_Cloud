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
        # Initializes the manager with the database file and creates the necessary table.
        self.object_manager = ObjectManager(db_file=db_file)
        self.object_name = "MultipartUpload"
        self.object_manager.object_manager.create_management_table('MultipartUpload', MultipartUploadModel.TABLE_STRUCTURE)


    def create_multipart_upload(self, multipart_upload: MultipartUploadModel) -> str:
        """Creates a multipart upload process and returns a unique UploadId."""
        if not isinstance(multipart_upload, MultipartUploadModel):
            raise TypeError('Expected an instance of MultipartUploadModel')
        self.object_manager.save_in_memory(self.object_name, multipart_upload.to_sql())
        return multipart_upload.upload_id

    def upload_part(self, multipart_upload: MultipartUploadModel, new_part: PartModel, body: str) -> str:
        if isinstance(multipart_upload.parts, str):
            multipart_upload.parts = json.loads(multipart_upload.parts)

        # Updates the upload object with the new part
        multipart_upload.parts.append({
            'PartNumber': new_part.part_number,
            'FilePath': new_part.part_file_path
        })
        # # Update in the database
        str_parts = json.dumps(multipart_upload.parts)
        update_statement = f"parts = '{str_parts}'"
        criteria = f"object_id = '{multipart_upload.upload_id}'"
        self.object_manager.update_in_memory(self.object_name, updates=update_statement, criteria=criteria)
        # # Generate a fake ETag for the example
        return f'etag_{new_part.part_number}'

    def complete_multipart_upload(self, multipart_upload: MultipartUploadModel):
        # Deletes the multipart upload record from memory upon completion.
        self.object_manager.delete_from_memory_by_pk(self.object_name, multipart_upload.pk_column, multipart_upload.pk_value)





  


 
    

