from Abc import DBO
class InstanceService(DBO):
    def create(self, db_instance_identifier: str, allocated_storage: int = None, port: int = 3306, region: str = 'a', is_replica: bool = False, 
                 backup_auto: bool = False,  region_auto_backup: str = '',  
                 master_username: str = None,  master_user_password: str = None, databases: dict = None,  db_name: str = None,  path_file: str = None):
        '''Implement logic to create a Instance.'''
        pass

    def delete(self,db_instance_identifier: str):
        '''Delete an existing Instance object.'''
        pass

    def describe(self,db_instance_identifier: str,):
        '''Describe the details of a Instance object.'''
        pass

    def modify(self,db_instance_identifier: str,):
        '''Modify an existing Instance object.'''
        pass
