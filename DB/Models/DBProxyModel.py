
from typing import Dict
from datetime import datetime
class DBProxyModel:
    def __init__(self, db_proxy_name:str, auth:Dict,  require_TLS:bool = False,
    idle_client_timeout:int=123,
    debug_logging:bool=False) -> None:
        self.db_proxy_name = db_proxy_name
        self.auth = auth
        self.require_TLS = require_TLS
        self.idle_client_timeout = idle_client_timeout
        self.debug_logging = debug_logging
        self.creation_date = datetime.now()
        self.updation_date = None
    

    def to_dict(self) -> Dict:
        return {
            'db_proxy_name':self.db_proxy_name,
            'auth':self.auth,
            'require_TLS': self.require_TLS,
            'idle_client_timeout': self.idle_client_timeout,
            'debug_logging': self.debug_logging,
            'creation_date':self.creation_date
        }

