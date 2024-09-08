from VesionModel import Version
from EncryptionModel import EncryptionModel
from Tag import Tag
from AclModel import Acl

class ObjectModel:
    def __init__(self, bucket_name: str, object_key: str):
        self.bucket = bucket_name
        self.key = object_key
        self.tagging:list[Tag] = []
        self.versioning: list[Version] = []
        self.acl = Acl()
        self.encryption = EncryptionModel()

    def to_dict(self):
        return {
            "Bucket": self.bucket,
            "Key": self.key,
            "TagSet": [tag.to_dict() for tag in self.tagging],
            "Versions": [version.to_dict() for version in self.versioning],
            "Acl": self.acl.to_dict(),
            "Encryption": self.encryption.to_dict(),
        }
