from datetime import datetime
import hashlib

# domain/versioning.py
class Version:
    def __init__(self, version_id: str):
        self.version_id = version_id
        self.etag = hashlib.md5(version_id).hexdigest()
        self.size = 0
        self.last_modified = datetime.utcnow()  # אתחול לתאריך ושעה של היום
        self.legal_hold = False
        # self.acl = 
        # self.retention =
        self.content_length = 0
        self.content_type = "text/plain"
        # self.meta_data = 
        # self.object_parts =
        # self.tags = 
