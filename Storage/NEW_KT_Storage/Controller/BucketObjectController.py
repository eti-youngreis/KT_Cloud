from Service import BucketObjectService

class BucketObjectController:
    def __init__(self, service: BucketObjectService):
        self.service = service


    def create_bucket_object(self, **kwargs):
        self.service.create(**kwargs)


    def delete_bucket_object(self):
        self.service.delete()


    # def modify_bucket_object(self, updates):
    #     self.service.modify(updates)
    

    def put_bucket_object(self, updates):
        self.service.put(updates)


    def get_bucket_object(self):
        self.service.get()