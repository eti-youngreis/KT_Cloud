from typing import Dict, Any
import os
import aiofiles
import shutil


URL_SERVER = "D:\\בוטקמפ\\server"
class StorageManager:
   """here will be storage actions - S3/localFileSystem"""

   def __init__(self, server_path=URL_SERVER) -> None:
      self.server_path = server_path
      
   def create(self, bucket, key, version_id, data) -> None:
      """Creates a new file with the specified content."""
   
      # Create the file path with the new version ID before the extension
      file_name, file_extension = os.path.splitext(key)
      versioned_file_name = f"{file_name}.v{version_id}{file_extension}"
      file_path = os.path.join(self.server_path, bucket, versioned_file_name)

      # Create the directory structure if it doesn't exist
      os.makedirs(os.path.dirname(file_path), exist_ok=True)
      # Write the body to the file
      with aiofiles.open(file_path, "wb") as f:
         f.write(data)

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
      
   
   def delete_by_name(self, bucket_name, key) -> None:
      file_path = self.server_path / bucket_name / key
      if file_path.exists():
         os.remove(file_path)
         
   def rename(self, bucket_name, old_key, new_key, version_id) -> None:
      
      """Renames a file in a specified bucket and version."""
      old_file_name, old_file_extension = os.path.splitext(old_key)
      old_versioned_file_name = f"{old_file_name}.v{version_id}{old_file_extension}"
      old_file_path = os.path.join(self.server_path, bucket_name, old_versioned_file_name)
      
      new_file_name, new_file_extension = os.path.splitext(new_key)
      new_versioned_file_name = f"{new_file_name}.v{version_id}{new_file_extension}"
      new_file_path = os.path.join(self.server_path, bucket_name, new_versioned_file_name)

      if os.path.exists(old_file_path):
         os.rename(old_file_path, new_file_path)

def copy(self, source_bucket_name, source_key, source_version_id,
            target_bucket_name, target_key, target_version_id) -> None:
      """Copies a file from one bucket and version to another."""
      source_file_name, source_file_extension = os.path.splitext(source_key)
      source_versioned_file_name = f"{source_file_name}.v{source_version_id}{source_file_extension}"
      source_file_path = os.path.join(self.server_path, source_bucket_name, source_versioned_file_name)

      target_file_name, target_file_extension = os.path.splitext(target_key)
      target_versioned_file_name = f"{target_file_name}.v{target_version_id}{target_file_extension}"
      target_file_path = os.path.join(self.server_path, target_bucket_name, target_versioned_file_name)
      
      if os.path.exists(source_file_path):
         os.makedirs(os.path.dirname(target_file_path), exist_ok=True)
         shutil.copyfile(source_file_path, target_file_path)

