
from typing import Dict, List
from exceptions.db_proxy_exception import *
from datetime import datetime
from DBProxyTargetGroupModel import TargetGroupModel
MAX_TARGET_GROUPS = 10
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
        self.target_groups = {}
    

    def to_dict(self) -> Dict:
        return {
            'db_proxy_name':self.db_proxy_name,
            'auth':self.auth,
            'require_TLS': self.require_TLS,
            'idle_client_timeout': self.idle_client_timeout,
            'debug_logging': self.debug_logging,
            'creation_date':self.creation_date
        }
    
    
    def register_targets(self, target_group_name: str = None, db_instance_identifiers: List[str]=[], db_cluster_identifiers: List[str]=[]):
        """
        Registers DB instances and DB clusters to the specified or new target group.

        Parameters:
        - target_group_name (str): The name of the target group. If not provided, a new target group is created for each target.
        - db_instance_identifiers (List[str]): List of DB instance identifiers to be registered.
        - db_cluster_identifiers (List[str]): List of DB cluster identifiers to be registered.
        """
        if target_group_name not in self.target_groups:
            raise DBProxyTargetGroupNotFoundFault(f'{target_group_name} not found as a target group in db proxy {self.db_proxy_name}')
        targets_description = []
        def create_and_register(target_id, is_cluster):
            tg_name = target_group_name or f"target_{target_id}"
            target_group = self.target_groups.get(tg_name, TargetGroupModel(tg_name, False))
            targets_description.append(target_group.register_target(target_id, is_cluster))
            if not target_group_name:
                self.target_groups[tg_name] = target_group

        for target_id in db_instance_identifiers:
            create_and_register(target_id, False)
        for target_id in db_cluster_identifiers:
            create_and_register(target_id, True)
        return targets_description

    
    def deregister_targets(self, target_group_name: str, db_instance_identifiers: List[str] = [], db_cluster_identifiers: List[str] = []):
        """
        Deregisters specified DB instances and clusters from the given target group.

        Parameters:
        - target_group_name (str): The name of the target group from which to deregister targets.
        - db_instance_identifiers (List[str]): List of DB instance identifiers to deregister.
        - db_cluster_identifiers (List[str]): List of DB cluster identifiers to deregister.

        Raises:
        - DBProxyTargetGroupNotFoundFault: If the target group name is not found.
        """
        if target_group_name not in self.target_groups:
            raise DBProxyTargetGroupNotFoundFault(f'{target_group_name} not found in db proxy {self.db_proxy_name}')
    
        target_group = self.target_groups[target_group_name]
        for target_id in db_cluster_identifiers + db_instance_identifiers:
            target_group.deregister_target(target_id)

        
        
        

