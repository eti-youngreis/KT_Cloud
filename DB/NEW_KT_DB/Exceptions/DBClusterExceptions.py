from GeneralExeptions import ObjectNotFoundException

class DBClusterNotFoundException(ObjectNotFoundException):
    def __init__(self):
        super().__init__('DB Cluster')