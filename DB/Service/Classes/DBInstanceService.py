from Abc import DBO
from DataAccess.DBManager import DBManager
from Models.db_instance import DBInstance
from Validation.Validation import is_valid_db_instance_identifier


class InstanceService(DBO):
    def __init__(self, dal: DBManager):
        self.dal = dal

    def create(self, db_instance_identifier: str, source_db_instance_identifier: str, allocated_storage: int = None, port: int = 3306, region: str = 'a', is_replica: bool = False,
               backup_auto: bool = False,  region_auto_backup: str = '',
               master_username: str = None,  master_user_password: str = None, databases: dict = None,  db_name: str = None,  path_file: str = None):
        '''Implement logic to create a Instance.'''
        if not is_valid_db_instance_identifier(db_instance_identifier, 255):
            raise ValueError('the identifier is not valid')
        instance = DBInstance(db_instance_identifier=db_instance_identifier, allocated_storage=allocated_storage, port=port, region=region, is_replica=is_replica, backup_auto=backup_auto,
                              region_auto_backup=region_auto_backup, master_username=master_username, master_user_password=master_user_password, databases=databases, db_name=db_name, path_file=path_file)
        if is_replica and source_db_instance_identifier:
            source_db_instance = self.get_db_instance_by_id(source_db_instance_identifier)
            source_db_instance.replicas += instance
            self.dal.update('DBInstance', source_db_instance.db_instance_identifier, source_db_instance.to_dict())
        self.dal.insert('DBInstance',instance.to_dict())
        return instance

    def delete(self, db_instance_identifier: str):
        '''Delete an existing Instance object.'''
        pass

    def describe(self, db_instance_identifier: str,):
        '''Describe the details of a Instance object.'''
        pass

    def modify(self, db_instance_identifier: str,):
        '''Modify an existing Instance object.'''
        pass

    def switchover(self, db_instance_identifier: str):
        instance = self.get_db_instance_by_id(db_instance_identifier)
        instance_replica=self.get_db_instance_replica(instance)
        instance.status='waiting'
        instance_replica.is_promote=True
    
    def get_db_instance_by_id(self,instance_id):
        instance_by_id=self.dal.select('DBInstance',instance_id)
        if instance_by_id is None:
            raise ValueError("instance does not exist")
        return instance_by_id

    def get_db_instance_replica(instance:DBInstance):
        if instance.replicas=={}:
            raise RuntimeError('can not switchover with out replica in this instance')
        return instance.replicas[0]