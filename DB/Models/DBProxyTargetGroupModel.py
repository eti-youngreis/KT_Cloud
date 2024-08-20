from typing import Dict
from datetime import datetime
class DBProxyTargetModel:
    def __init__(self, db_proxy_name:str,
            target_group_name:str,
            target_group_arn:str,
            is_default: bool,
            status:str,
            created_date: datetime,
            ) -> None:
        self.db_proxy_name = db_proxy_name,
        self.target_group_name = target_group_name
        self.target_group_arn = target_group_arn
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
        self.created_date = created_date
        self.updated_date = self.created_date
    

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