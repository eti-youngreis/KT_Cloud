from DB.NEW_KT_DB.Service.Classes.DBInstanceService import DBInstanceService
"""
DBInstanceController

This class serves as a controller for managing database instances. It provides an interface
for creating, deleting, describing, modifying, and managing the state of database instances.

The controller delegates most of its operations to a DBInstanceService.

Methods:
    create_db_instance: Create a new database instance.
    delete_db_instance: Delete an existing database instance.
    describe_db_instance: Get a description of a database instance.
    modify_db_instance: Modify an existing database instance.
    get_db_instance: Retrieve a specific database instance.
    stop_db_instance: Stop a running database instance.
    start_db_instance: Start a stopped database instance.
    create_snapshot: Create a snapshot of a database instance.
    delete_snapshot: Delete a snapshot of a database instance.
    restore_version: Restore a database instance to a specific version.
"""

class DBInstanceController:
    def __init__(self, service: DBInstanceService):
        self.service = service

    def create_db_instance(self, **kwargs):
        return self.service.create(**kwargs)

    def delete_db_instance(self, db_instance_identifier):
        return self.service.delete(db_instance_identifier)

    def describe_db_instance(self, db_instance_identifier):
        return self.service.describe(db_instance_identifier)

    def modify_db_instance(self, db_instance_identifier, **kwargs):
        return self.service.modify(db_instance_identifier, **kwargs)
    
    def get_db_instance(self, db_instance_identifier):
        return self.service.get(db_instance_identifier)

    def stop_db_instance(self, db_instance_identifier):
        return self.service.stop(db_instance_identifier)

    def start_db_instance(self, db_instance_identifier):
        return self.service.start(db_instance_identifier)    
    
    def execute_query(self, db_instance_identifier, query, db_name):
        return self.service.execute_query(db_instance_identifier, query, db_name)

    def create_snapshot(self, db_instance_identifier, db_snapshot_identifier):
        return self.service.create_snapshot(db_instance_identifier, db_snapshot_identifier)

    def delete_snapshot(self, db_snapshot_identifier):
        return self.service.delete_snapshot(db_snapshot_identifier)

    def restore_version(self, db_instance_identifier, db_snapshot_identifier):
        return self.service.restore_version(db_instance_identifier, db_snapshot_identifier)

