from typing import Dict
from datetime import datetime
from exceptions.db_proxy_exception import *
from DBProxyTargetModel import TargetModel
MAX_TARGETS = 10
class TargetGroupModel:
    def __init__(self, db_proxy_name:str,
            target_group_name:str,
            is_default: bool = True,
            status:str = "available",
            ) -> None:
        self.db_proxy_name = db_proxy_name,
        self.target_group_name = target_group_name
        self.target_group_arn = None
        self.is_default = is_default
        self.status = status
        self.connection_pool_config = {
                'MaxConnectionsPercent': 123,
                'MaxIdleConnectionsPercent': 123,
                'ConnectionBorrowTimeout': 123,
                'SessionPinningFilters': [
                    'string',
                ],
                'InitQuery': 'string'
            }
        self.created_date = datetime.now()
        self.updated_date = self.created_date
        self.targets = {}
    

    def to_dict(self) -> Dict:
        return {
            'db_proxy_name':self.db_proxy_name,
            'target_group_name':self.target_group_name,
            'target_group_arn': self.target_group_arn,
            'is_default': self.is_default,
            'status': self.status,
            'connection_pool_config':self.connection_pool_config,
            'created_date': self.created_date,
            'updated_date':self.updated_date
            
        }
        
    def register_target(self, target_identifier:str, is_cluster_flag:bool):
        if is_cluster_flag:
            pass #check if there is a cluster with this identifier
        else:
            pass #check if there is a db instance with this identifier
        target = TargetModel("","","",0,'','')
        self.targets[target_identifier] = target
        return target.to_dict()
    
    def deregister_target(self, target_identifier:str) -> None:
        if target_identifier not in self.targets:
            raise DBProxyTargetNotFoundFault(f'target {target_identifier} not found in target group {self.target_group_name} in proxy {self.db_proxy_name}')
        del self.targets[target_identifier]
