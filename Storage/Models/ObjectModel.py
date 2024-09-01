from .VesionModel import Version
from .EncryptionModel import EncryptionModel
from Tag import Tag

class ObjectModel:
    def __init__(self, bucket_name: str, object_key: str):
        self.bucket = bucket_name
        self.key = object_key
        # self.tagging = Tagging()
        self.versioning:list[Version] = []
        self.encryption = EncryptionModel()
        # self.acl = ACL()
