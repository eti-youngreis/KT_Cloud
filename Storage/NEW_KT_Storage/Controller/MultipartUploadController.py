import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from Service.Classes.MultiPartUploadService import MultipartUploadService

class MultipartUploadController:
    def __init__(self, db_file: str, storage_path: str):
        # Initializes the controller with the database file and storage path.
        self.service = MultipartUploadService(db_file, storage_path)
    
    def initiate_upload(self, bucket_name: str, object_key: str) -> str:
        """
        Initiates a multipart file upload and returns the upload_id.
        """
        return self.service.initiate_multipart_upload(bucket_name, object_key)
    
    def upload_file_parts(self, upload_id: str, file_path: str, part_size: int = 1024):
        """
        Uploads the parts of the file based on the upload_id.
        """
        self.service.upload_file_parts(upload_id, file_path, part_size)

    def list_parts(self, upload_id: str):
        # Retrieves the list of parts associated with the given upload_id.
        return self.service.list_parts(upload_id)
    
    def complete_upload(self, upload_id: str, bucket_name: str = None, object_key: str = None) -> str:
        """
        Completes the upload process and merges all parts into a single file.
        """
        return self.service.complete_upload(upload_id, bucket_name, object_key)
    

# Example usage of the controller
if __name__ == "__main__":
    db_file = "my_db.db"
    storage_path = "C:/Users/shana/Desktop/a"
    
    controller = MultipartUploadController(db_file, storage_path)
    
    # Initiates the upload
    bucket_name = "my_bucket"
    object_key = "my_file_shani_3.txt"
    # upload_id = controller.initiate_upload(bucket_name, object_key)
    # print(f"Upload ID: {upload_id}")
    
    # # Uploads the file parts
    # # controller.service.upload_part(upload_id, 1, "aaaa")
    # # controller.service.upload_part(upload_id, 2, "bbbb")
    # file_path = "C:/Users/shana/Desktop/a/my_file.txt"
    # controller.upload_file_parts(upload_id, file_path)
    # print("File parts uploaded.")

    # list_parts = controller.list_parts(upload_id)
    # print(list_parts, "list_parts")

    # # Completes the upload
    complete_file_path = controller.complete_upload('af9f23ac-007a-4b98-9238-43fe4c7a2c56', bucket_name, object_key)
    print(f"Upload complete! File saved at: {complete_file_path}")

    # Displays all existing multipart uploads
    all_uploads = controller.service.select_all_from_table(db_file, "mng_MultipartUploads")
    print("All multipart uploads:", all_uploads)
