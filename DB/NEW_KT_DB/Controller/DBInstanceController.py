import datetime
from typing import Optional, Dict
from Service.Classes.DBInstanceService import DBInstanceService


class DBInstanceController:
    def __init__(self, service: DBInstanceService):
        self.service = service

    def create_db_instance(self,**kwargs):
        return self.service.create(**kwargs)

    def delete_db_instance(self, db_instance_identifier: str):
        self.service.delete(db_instance_identifier)

    def modify_db_instance(self, **kwargs):
        return self.service.modify(**kwargs)

    def describe_db_instance(self, db_instance_identifier: str):
        return self.service.describe(db_instance_identifier)

