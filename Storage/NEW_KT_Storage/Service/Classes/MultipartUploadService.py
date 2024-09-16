from Models  import MultipartUploadModel , PartModel
from DataAccess import MultipartUploadManager   

class MultipartUploadService:
    def __init__(self):
        self.manager = MultipartUploadManager()

    def create_multipart_upload(self, bucket_name: str, object_key: str) -> str:
        """יוזם את תהליך ההעלאה עבור bucket ואובייקט"""
        multipart_upload = MultipartUploadModel(bucket_name=bucket_name, object_key=object_key)
        return self.manager.create_multipart_upload(multipart_upload)

    def upload_part(self, bucket_name: str, object_key: str, upload_id: str, part_number: int, body: bytes) -> str:
        """מעלה חלק של קובץ עבור bucket ואובייקט ומחזיר את ETag של החלק"""
        new_part = PartModel(part_number=part_number, body=body)
        return self.manager.upload_part(bucket_name, object_key, upload_id, new_part)

    def complete_upload(self, bucket_name: str, object_key: str, upload_id: str):
        """משלים את תהליך ההעלאה"""
        self.manager.complete_multipart_upload(bucket_name, object_key, upload_id)