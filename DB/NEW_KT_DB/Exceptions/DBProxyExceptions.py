

class DBProxyAlreadyExistsFault(Exception):
    def __init__(self):
        super().__init__('DB Proxy already exist!')


class DBProxyNotFoundFault(Exception):
    def __init__(self):
        super().__init__('DB Proxy not found!')

