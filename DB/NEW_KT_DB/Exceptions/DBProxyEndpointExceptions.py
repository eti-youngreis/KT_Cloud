from DB.NEW_KT_DB.Exceptions.GeneralExeptions import *
from DB.NEW_KT_DB.Models.DBProxyEndpointModel import DBProxyEndpoint


class DBProxyNotFoundException(ObjectNotFoundException):
    def __init__(self, endpoint_name: str):
        super().__init__(DBProxyEndpoint.object_name, endpoint_name)

class DBProxyEndpointNotFoundException(ObjectNotFoundException):
    def __init__(self, endpoint_name: str):
        super().__init__(DBProxyEndpoint.object_name, endpoint_name)

class DBProxyNotFoundException(ObjectNotFoundException):
    def __init__(self, db_proxy_name: str):
        super().__init__(DBProxyEndpoint.object_name, db_proxy_name)

class DBProxyEndpointAlreadyExistsException(ObjectAlreadyExistsException):
    def __init__(self, endpoint_name: str):
        super().__init__(DBProxyEndpoint.object_name, endpoint_name)

class InvalidDBProxyStateException(InvalidObjectStateException):
    def __init__(self, endpoint_name: str, object_state:str):
        super().__init__(DBProxyEndpoint.object_name, endpoint_name, 'available', object_state)

class InvalidDBProxyEndpointStateException(InvalidObjectStateException):
    def __init__(self, endpoint_name: str, object_state:str):
        super().__init__(DBProxyEndpoint.object_name, endpoint_name, 'available', object_state)



