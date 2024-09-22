from GeneralExeptions import ObjectNotFoundException

class BucketObjectNotFoundException(ObjectNotFoundException):
    def __init__(self):
        super().__init__('Bucket Object')