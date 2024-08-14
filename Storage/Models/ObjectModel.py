from .VesionModel import Versioning

class ObjectModel:
    def __init__(self, bucket_name, object_key ):
        self.bucket = bucket_name
        self.key = object_key
        # self.tagging = Tagging()
        self.versioning = Versioning()
        # self.acl = ACL()
    