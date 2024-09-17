from Storage.NEW_KT_Storage.Service.Classes.BucketObjectService import BucketObjectService


class BucketObjectController:
    def __init__(self, service: BucketObjectService):
        self.service = service


    def create_bucket_object(self, **kwargs):
        self.service.create(**kwargs)


    def delete_bucket_object(self,bucket_name,object_key,version_id=None):
        self.service.delete(bucket_name,object_key,version_id)


    # def modify_bucket_object(self, updates):
    #     self.service.modify(updates)
    

    def put_bucket_object(self, **kwargs):
        self.service.put(**kwargs)


    def get_bucket_object(self,bucket_name,object_name,version_id=None):
        return self.service.get(bucket_name,object_name,version_id)

    def list_all_objects(self,bucket_name):
        return self.service.get_all(bucket_name)


object_service=BucketObjectService()
object_controller=BucketObjectController(object_service)
# create
# object_controller.create_bucket_object(bucket_name='my_bucket', object_key='pppp')
# get
print(object_controller.get_bucket_object("my_bucket",None))
# delete
# object_controller.delete_bucket_object(bucket_name='my_bucket',object_key=None)

# get_all
# print("all objects in bucket",object_controller.list_all_objects(None))

#put
# object_controller.put_bucket_object(bucket_name="my_bucket",object_key="my_object100",content="gfff")
