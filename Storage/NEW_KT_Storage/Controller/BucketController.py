from Storage.NEW_KT_Storage.Service.Classes.BucketService import BucketService

class BucketController:

    def __init__(self, service: BucketService):
        self.service = service

    def create_bucket(self, bucket_name: str, owner: str,region="USA"):
        self.service.create(bucket_name, owner,region)

    def delete_bucket(self, bucket_name):
        self.service.delete(bucket_name)

    def get_bucket(self, bucket_name):
       return self.service.get(bucket_name)

def main():
    bucket_service = BucketService()

    bucket_controller = BucketController(service=bucket_service)

    bucket_controller.create_bucket("Malki-bucket", "Malki")
    # print(bucket_controller.get_bucket("Malkim-bucket"))
    # bucket_controller.delete_bucket("check_bucket")

main()

