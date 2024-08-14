import os
from typing import Dict, Any

class StorageManager:
    def __init__(self):
       self.server_path="s3 project/KT_Cloud/Storage/server"
    """here will be storage actions - S3/localFileSystem"""
    def create(self, bucket, key, version_id, data) -> None:
      """Creates a new file with the specified content."""

      file_path = os.path.join(self.server_path, bucket, key)

      if key.endswith('/'):
         # Create a directory
         os.makedirs(file_path, exist_ok=True)
         print(f"Directory '{key}' created in bucket '{bucket}'.")
      else:
         # Create a file
         file_name, file_extension = os.path.splitext(key)
         versioned_file_name = f"{file_name}.v{version_id}{file_extension}"
         file_path = os.path.join(self.server_path, bucket, versioned_file_name)

         os.makedirs(os.path.dirname(file_path), exist_ok=True)  # Ensure the directory exists
         with open(file_path, "wb") as f:
            f.write(data)

         print(f"File '{key}' created in bucket '{bucket}' with version '{version_id}'.")

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

    def delete(self) -> None:
      pass
    
    def rename(self)->None:
      pass
    
    def copy(self)->None:
      pass


