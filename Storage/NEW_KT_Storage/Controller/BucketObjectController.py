from Storage.NEW_KT_Storage.Service.Classes.BucketObjectService import BucketObjectService


class BucketObjectController:
    def __init__(self, service: BucketObjectService):
        self.service = service

    def create_bucket_object(self, **kwargs):
        self.service.create(**kwargs)

    def delete_bucket_object(self, bucket_name, object_key, version_id=None):
        self.service.delete(bucket_name, object_key, version_id)

    def put_bucket_object(self, **kwargs):
        self.service.put(**kwargs)

    def get_bucket_object(self, bucket_name, object_name, version_id=None):
        return self.service.get(bucket_name, object_name, version_id)

    def list_all_objects(self, bucket_name):
        return self.service.get_all(bucket_name)


