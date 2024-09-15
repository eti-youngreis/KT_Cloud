from Service import DBInstanceService

class DBInstanceController:
    def __init__(self, service: DBInstanceService):
        self.service = service

    def create_db_instance(self, **kwargs):
        return self.service.create(**kwargs)

    def create_snapshot(self, db_instance_identifier, db_snapshot_identifier):
        return self.service.create_snapshot(db_instance_identifier, db_snapshot_identifier)

    def restore_version(self, db_instance_identifier, db_snapshot_identifier):
        return self.service.restore_version(db_instance_identifier, db_snapshot_identifier)

    def get_endpoint(self, db_instance_identifier):
        return self.service.get_endpoint(db_instance_identifier)

    def stop_db_instance(self, db_instance_identifier):
        return self.service.stop(db_instance_identifier)

    def start_db_instance(self, db_instance_identifier):
        return self.service.start(db_instance_identifier)
