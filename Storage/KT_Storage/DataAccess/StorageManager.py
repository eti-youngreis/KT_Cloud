import ctypes
from typing import Dict, Any
import os
import aiofiles
import shutil
from Cryptodome.Cipher import AES
from Cryptodome.Util.Padding import pad
# import base6import


URL_SERVER = 's3/KT_cloud/Storage/server'
class StorageManager:
   """here will be storage actions - S3/localFileSystem"""

   def __init__(self, server_path=URL_SERVER) -> None:
      self.server_path = server_path

   def create_bucket(self, bucket_name) -> None:
      """Creates a new bucket"""
      file_path = os.path.join(self.server_path, bucket_name)
      os.makedirs(file_path, exist_ok=True)
      
   def create(self, bucket, key, data, version_id) -> None:
      """Creates a new file with the specified content"""
      
      file_path = os.path.join(self.server_path, bucket, key)
      
      if key.endswith('/'):
         # Create a directory
         os.makedirs(file_path, exist_ok=True)
         print(f"Directory '{key}' created in bucket '{bucket}'.")
      else:
         # Create a file
         file_name, file_extension = os.path.splitext(key)
         versioned_file_name = f'{file_name}.v{version_id}{file_extension}'
         file_path = os.path.join(self.server_path, bucket, versioned_file_name)
         
         os.makedirs(os.path.dirname(file_path), exist_ok=True)  # Ensure the directory exists
         with open(file_path, 'wb') as f:
            f.write(data)
         print(f"File '{key}' created in bucket '{bucket}' with version '{version_id}'.")

   def get(self, bucket, key , version_id) -> Dict[str, Any]:
      
      """Retrieves the content of a specified file in a bucket and version."""
      file_name, file_extension = os.path.splitext(key)
      versioned_file_name = f"{file_name}.v{version_id}{file_extension}"
      file_path = os.path.join(self.server_path, bucket, versioned_file_name)
      
      if not os.path.exists(file_path):
         raise FileNotFoundError(f"File '{key}' with version '{version_id}' not found in bucket '{bucket}'.")
      
      if os.path.isdir(file_path):
         # If the object is a directory, return its metadata and list of contents
         contents = os.listdir(file_path)
         return {
            'object_type': 'directory',
            'path': file_path,
            'contents': contents,
            'last_modified': os.path.getmtime(file_path)
         }
      else:
         # If the object is a file, return its content and metadata
         with open(file_path, 'rb') as f:
            content = f.read()
         return {
            'object_type': 'file',
            'file_name': key,
            'version': version_id,
            'content': content,
            'file_size': os.path.getsize(file_path),
            'last_modified': os.path.getmtime(file_path)
         }
      
   
   def delete_by_name(self, bucket_name, version_id,  key) -> None:
      """Delete a specified file or directory by name in a bucket and version."""
      file_name, file_extension = os.path.splitext(key)
      file_name_path = f"{file_name}.v{version_id}{file_extension}"
      file_path = os.path.join(self.server_path, bucket_name, file_name_path)
      
      if os.path.exists(file_path):
         
         if os.path.isdir(file_path):
            shutil.rmtree(file_path)  # Remove directory and its contents
            print(f"Directory '{key}' with version '{version_id}' deleted from bucket '{bucket_name}'.")
            
         else:
            os.remove(file_path)
            print(f"File '{key}' with version '{version_id}' deleted from bucket '{bucket_name}'.")
            
      else:
         print(f"Object '{key}' with version '{version_id}' not found in bucket '{bucket_name}'.")

   def encript_version(self, bucket, key, version) -> None:
      
      # Preparing the file for the encrypted version
      file_name, file_extension = os.path.splitext(key)
      encrypted_version = self._encrypt(version)
      versioned_file_name = f"{file_name}.v{encrypted_version}{file_extension}"
      file_path = os.path.join(self.server_path, bucket, versioned_file_name) 
      
      # Update the file attribute (make it hidden)
      result = ctypes.windll.kernel32.SetFileAttributesW(str(file_path), 0x02)
      if result:
         print(f"File '{versioned_file_name}' in bucket '{bucket}' has been encrypted and hidden.")
      else:
         print(f"Failed to set hidden attribute on '{versioned_file_name}' in bucket '{bucket}'.")  
         
   def _encrypt(self, version_id):
      # Encryption of the version number
      cipher = AES.new(self.key, AES.MODE_CBC)
      iv = cipher.iv
      encrypted_version = cipher.encrypt(pad(version_id.encode('utf-8'), AES.block_size))
      return base64.b64encode(iv + encrypted_version).decode('utf-8')
   
   def decrypt_version(self, encrypted_version):
      """Function to decrypt the encrypted version"""
      encrypted_version = base64.b64decode(encrypted_version)
      iv = encrypted_version[:AES.block_size]
      cipher = AES.new(self.key, AES.MODE_CBC, iv)
      decrypted_version = unpad(cipher.decrypt(encrypted_version[AES.block_size:]), AES.block_size)
      return decrypted_version.decode('utf-8')    
   def rename(self, bucket_name, old_key, new_key, version_id) -> None:
      
      """Rename a file in a specified bucket and version."""
      old_file_name, old_file_extension = os.path.splitext(old_key)
      old_versioned_file_name = f"{old_file_name}.v{version_id}{old_file_extension}"
      old_file_path = os.path.join(self.server_path, bucket_name, old_versioned_file_name)
      
      new_file_name, new_file_extension = os.path.splitext(new_key)
      new_versioned_file_name = f"{new_file_name}.v{version_id}{new_file_extension}"
      new_file_path = os.path.join(self.server_path, bucket_name, new_versioned_file_name)

      if os.path.exists(old_file_path):
         os.rename(old_file_path, new_file_path)
         print(f"Object '{old_key}' renamed to '{new_key}' in bucket '{bucket_name}' with version '{version_id}'.")
         
      else:
         print(f"Object '{old_key}' with version '{version_id}' not found in bucket '{bucket_name}'.")


   def copy(self, source_bucket_name, source_key,source_version_id,
            target_bucket_name, target_key) -> None:
      """Copy a file from one bucket to another with optional source version."""
      # Construct source file path
      source_file_name, source_file_extension = os.path.splitext(source_key)
      source_versioned_file_name = f"{source_file_name}.v{source_version_id}{source_file_extension}"
      source_file_path = os.path.join(self.server_path, source_bucket_name, source_versioned_file_name)

      # Construct target file path (without versioning)
      target_file_name, target_file_extension = os.path.splitext(target_key)
      target_file_path = os.path.join(self.server_path, target_bucket_name, f"{target_file_name}{target_file_extension}")

      if os.path.exists(source_file_path):
         if os.path.isdir(source_file_path):
            shutil.copytree(source_file_path, target_file_path)  # Copy directory
            print(f"Directory '{source_key}' from bucket '{source_bucket_name}' with version '{source_version_id}' "
                  f"copied to '{target_key}' in bucket '{target_bucket_name}'.")
         else:
            os.makedirs(os.path.dirname(target_file_path), exist_ok=True)  # Ensure target directory exists
            shutil.copyfile(source_file_path, target_file_path)  # Copy file
            print(f"File '{source_key}' from bucket '{source_bucket_name}' with version '{source_version_id}' "
                  f"copied to '{target_key}' in bucket '{target_bucket_name}'.")
      else:
         print(f"Source object '{source_key}' with version '{source_version_id}' not found in bucket '{source_bucket_name}'.")
         
         





