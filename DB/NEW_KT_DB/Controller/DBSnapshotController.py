from Service import DBSnapshotService

class DBSnapshotController:
    def __init__(self, service: DBSnapshotService):
        self.service = service


    def create_db_snapshot(self, db_name: str, description: str = None, progress: str = None):
        self.service.create(db_name, description, progress)


    def delete_db_snapshot(self):
        self.service.delete()


    def modify_db_snapshot(self, owner_alias: str = None, status: str = None,
                           description: str = None, progress: str = None):
        self.service.modify(owner_alias, status, description, progress)
    