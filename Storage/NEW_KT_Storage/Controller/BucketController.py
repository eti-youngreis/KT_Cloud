from Storage.NEW_KT_Storage.Service.Classes.BucketService import BucketService

class BucketController:

    def __init__(self):
        self.service = BucketService()

    def create_bucket(self, bucket_name: str, owner: str,region=None):
        self.service.create(bucket_name, owner,region)

    def delete_bucket(self, bucket_name):
        self.service.delete(bucket_name)

    def get_bucket(self, bucket_name):
        return self.service.get(bucket_name)


