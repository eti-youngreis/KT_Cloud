from typing import Dict
class DBProxyTargetModel:
    def __init__(self, target_arn:str,
            endpoint:str,
            tracked_cluster_id:str,
            db_instance_id: str,
            port:int,
            type:str,
            role:str,
            ) -> None:
        self.target_arn = target_arn
        self.endpoint = endpoint
        self.tracked_cluster_id = tracked_cluster_id
        self.db_instance_id = db_instance_id
        self.port = port
        self.type = type #type need to be one of ['RDS_INSTANCE'|'RDS_SERVERLESS_ENDPOINT'|'TRACKED_CLUSTER']
        self.role = role #role need to be one of ['READ_WRITE'|'READ_ONLY'|'UNKNOWN']
        self.target_health = {'State':None, 'Reason':None, 'Description':None}
    

    def to_dict(self) -> Dict:
        return {
            'target_arn':self.target_arn,
            'endpoint':self.endpoint,
            'tracked_cluster_id': self.tracked_cluster_id,
            'db_instance_id': self.db_instance_id,
            'port': self.port,
            'type':self.type,
            'role': self.role,
            'target_health':self.target_health
            
        }