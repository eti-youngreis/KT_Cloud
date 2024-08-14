import os
import shutil

from typing import Dict, Any

class StorageManager:
    def __init__(self):
       self.server_path="C:/Users/shana/Desktop/server"
    """here will be storage actions - S3/localFileSystem"""
    def create(self) -> None:
      pass

    def get(self, bucket, key, version_id) -> Dict[str, Any]:

      """Retrieves the content of a specified file in a bucket and version."""
      file_name, file_extension = os.path.splitext(key)
      versioned_file_name = f"{file_name}.v{version_id}{file_extension}"
      file_path = os.path.join(self.server_path, bucket, versioned_file_name)

      if not os.path.exists(file_path):
         return {"error": "File not found"}

      with open(file_path, 'rb') as f:
         data = f.read()

      return data
      # return {"file_name": key, "version": version_id, "content": data}

    def delete_by_name(self, bucket_name, version_id,  key) -> None:
      """Deletes a specified file or directory by name in a bucket and version."""
      file_name, file_extension = os.path.splitext(key)
      versioned_file_name = f"{file_name}.v{version_id}{file_extension}"
      file_path = os.path.join(self.server_path, bucket_name, versioned_file_name)

      if os.path.exists(file_path):

         if os.path.isdir(file_path):
            shutil.rmtree(file_path)  # Remove directory and its contents
            print(f"Directory '{key}' with version '{version_id}' deleted from bucket '{bucket_name}'.")

         else:
            os.remove(file_path)
            print(f"File '{key}' with version '{version_id}' deleted from bucket '{bucket_name}'.")

      else:
         print(f"Object '{key}' with version '{version_id}' not found in bucket '{bucket_name}'.")

    def delete(self, bucket, key) -> None:
      pass

    def encript_version(self, bucket, key, version) -> None:
      pass
    
    def rename(self)->None:
      pass
    
    def copy(source_bucket, source_key, destination_bucket, destination_key, version_id=None)->None:
      pass


