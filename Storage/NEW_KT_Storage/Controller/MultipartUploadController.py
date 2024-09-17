import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from Service.Classes.MultiPartUploadService import MultipartUploadService
class MultipartUploadController:
    def __init__(self, db_file: str, storage_path: str):
        self.service = MultipartUploadService(db_file, storage_path)
    
    def initiate_upload(self, bucket_name: str, object_key: str) -> str:
        """
        יוזם העלאת קובץ מרובת חלקים ומחזיר את ה-upload_id.
        """
        return self.service.initiate_multipart_upload(bucket_name, object_key)
    
    def upload_file_parts(self, upload_id: str, file_path: str, part_size: int = 1024):
        """
        מעלה את חלקי הקובץ בהתבסס על ה-upload_id.
        """
        self.service.upload_file_parts(upload_id, file_path, part_size)

    def list_parts(self,upload_id: str):
        return self.service.list_parts(upload_id)
    
    def complete_upload(self, upload_id: str, bucket_name: str, object_key: str) -> str:
        """
        משלים את תהליך ההעלאה ומאחד את כל החלקים לקובץ אחד.
        """
        return self.service.complete_upload(upload_id, bucket_name, object_key)
    


# דוגמה לשימוש בקונטרולר
if __name__ == "__main__":
    db_file = "my_db.db"
    storage_path = "C:/Users/shana/Desktop/a"
    
    controller = MultipartUploadController(db_file, storage_path)
    
    # יוזם העלאה
    bucket_name = "my_bucket"
    object_key = "my_file.txt"
    upload_id = controller.initiate_upload(bucket_name, object_key)
    print(f"Upload ID: {upload_id}")
    
    # מעלה את חלקי הקובץ
    file_path = "C:/Users/shana/Desktop/a/my_file.txt"
    controller.upload_file_parts(upload_id, file_path)
    print("File parts uploaded.")

    list_parts=controller.list_parts(upload_id)
    print(list_parts,"list_parts")

    # משלים את ההעלאה
    complete_file_path = controller.complete_upload(upload_id, bucket_name, object_key)
    print(f"Upload complete! File saved at: {complete_file_path}")


        # מציג את כל ההעלאות מרובות החלקים הקיימות
    all_uploads = controller.service.select_all_from_table(db_file,"mng_MultipartUploads")
    print("All multipart uploads:", all_uploads)
    