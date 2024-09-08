import datetime
from typing import Optional, Dict
from Service.Classes.DBInstanceService import InstanceService


class DBinstanceController:
    def __init__(self, service: InstanceService):
        self.service = service

    def create_db_instance(self, db_instance_identifier: str,source_db_instance_identifier:str=None, allocated_storage: int = None, port: int = 3306, region: str = 'a',
                           source_instance: str = None, backup_auto: bool = False,  region_auto_backup: str = '',
                           master_username: str = None,  master_user_password: str = None, databases: dict = None,  db_name: str = None):
        self.service.create(db_instance_identifier=db_instance_identifier,source_db_instance_identifier=source_db_instance_identifier, allocated_storage=allocated_storage, port=port, region=region, source_instance=source_instance,
                            backup_auto=backup_auto, region_auto_backup=region_auto_backup, master_username=master_username, master_user_password=master_user_password, databases=databases, db_name=db_name)

    def delete_db_instance(self, db_instance_identifier: str):
        self.service.delete(db_instance_identifier)

    def modify_db_instance(self, db_instance_identifier: str):
        self.service.modify(db_instance_identifier)

    def describe_db_instance(self, db_instance_identifier: str):
        self.service.describe(db_instance_identifier)

    def switchover_db_instance(self,**kwargs):
        self.service.switchover()
