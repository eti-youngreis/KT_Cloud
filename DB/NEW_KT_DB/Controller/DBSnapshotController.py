from ..Service.Classes.DBInstanceService import DBInstanceService

class DBSnapshotController:
    def __init__(self, db_instance_service: DBInstanceService):
        self.db_instance_service = db_instance_service

    def create_snapshot(self, db_instance_identifier: str, db_snapshot_identifier: str):
        return self.db_instance_service.create_snapshot(db_instance_identifier, db_snapshot_identifier)

    def delete_snapshot(self, db_instance_identifier: str, db_snapshot_identifier: str):
        return self.db_instance_service.delete_snapshot(db_instance_identifier, db_snapshot_identifier)

    def restore_snapshot(self, db_instance_identifier: str, db_snapshot_identifier: str):
        return self.db_instance_service.restore_version(db_instance_identifier, db_snapshot_identifier)

    def list_snapshots(self, db_instance_identifier: str):
        db_instance = self.db_instance_service.get(db_instance_identifier)
        return list(db_instance._node_subSnapshot_name_to_id.keys())

    def describe_snapshot(self, db_instance_identifier: str, db_snapshot_identifier: str):
        return self.db_instance_service.describe_snapshot(db_instance_identifier, db_snapshot_identifier)

    def modify_snapshot(self, db_instance_identifier: str, db_snapshot_identifier: str, **kwargs):
        return self.db_instance_service.modify_snapshot(db_instance_identifier, db_snapshot_identifier, **kwargs)