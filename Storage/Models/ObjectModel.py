from .VesionModel import Version
from .EncryptionModel import EncryptionModel
class ObjectModel:
    def __init__(self, bucket_name, object_key ):
        self.bucket = bucket_name
        self.key = object_key
        # self.tagging = Tagging()
        self.versioning:list[Version] = []
        self.encryption = EncryptionModel()
        # self.acl = ACL()
    