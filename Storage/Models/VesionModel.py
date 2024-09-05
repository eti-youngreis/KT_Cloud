from datetime import datetime
import hashlib
from AclModel import Acl
from Tag import Tag


# domain/versioning.py
class Version:
    def __init__(self, version_id: str):
        self.version_id = version_id
        self.etag = hashlib.md5(version_id).hexdigest()
        self.size = 0
        self.last_modified = datetime.utcnow()  # אתחול לתאריך ושעה של היום
        self.legal_hold = False
        self.acl: Acl = Acl()
        # self.retention =
        self.content_length = 0
        self.content_type = "text/plain"
        # self.meta_data =
        # self.object_parts =
        self.tags: list[Tag] = []

    def to_dict(self):
        return {
            "Version_id": self.version_id,
            "Etag": self.etag,
            "Size": self.size,
            "Last_modified": self.last_modified,
            "Legal_hold": self.legal_hold,
            "Acl": self.acl.to_dict(),
            "Content_length": self.content_length,
            "Content_type": self.content_type,
            "Tags": [tag.to_dict() for tag in self.tags],
        }
