import sys
import os
# Add the project root directory to the Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
sys.path.append(project_root)
sys.path.append("D:/Users/רוט/Desktop/new project/KT_Cloud")

from DB.NEW_KT_DB.DataAccess.DBManager import DBManager
from DB.NEW_KT_DB.DataAccess.ObjectManager import ObjectManager
from DB.NEW_KT_DB.DataAccess.DBInstanceManager import DBInstanceManager
from DB.NEW_KT_DB.Service.Classes.DBInstanceService import DBInstanceService
from DB.NEW_KT_DB.Controller.DBSnapshotController import DBSnapshotController
from DB.NEW_KT_DB.Controller.DBInstanceController import DBInstanceController


def main():
    # Creating instances of required classes
    db_file = os.path.join('DB', 'NEW_KT_DB', 'test_database.db')
    db_manager = DBManager(db_file)
    object_manager = ObjectManager(db_file)
    db_instance_manager = DBInstanceManager(db_file)
    db_instance_service = DBInstanceService(db_instance_manager)
    db_instance_controller = DBInstanceController(db_instance_service)
    db_snapshot_controller = DBSnapshotController(db_instance_service)

    # Testing DBInstanceController functionality
    print("Testing DBInstanceController:")
    instance_id = "test-instance-1"
    db_instance = db_instance_controller.create_db_instance(
        db_instance_identifier=instance_id,
        allocated_storage=20,
        master_username="admin",
        master_user_password="password123",
        db_name="testdb"
    )
    print(f"DB instance created: {instance_id}")
   



    description = db_instance_controller.describe_db_instance(instance_id)
    print(f"DB instance description: {description}")

    db_instance_controller.modify_db_instance(instance_id, allocated_storage=30)
    print(f"DB instance modified: {instance_id}")

    db_instance_controller.stop_db_instance(instance_id)
    print(f"DB instance stopped: {instance_id}")

    db_instance_controller.start_db_instance(instance_id)
    print(f"DB instance started: {instance_id}")

    # Testing DBSnapshotController functionality
    print("\nTesting DBSnapshotController:")
    snapshot_id = "test-snapshot-1"
    db_snapshot_controller.create_snapshot(instance_id, snapshot_id)
    print(f"Snapshot created: {snapshot_id}")

    snapshots = db_snapshot_controller.list_snapshots(instance_id)
    print(f"List of snapshots: {snapshots}")

    snapshot_description = db_snapshot_controller.describe_snapshot(instance_id, snapshot_id)
    print(f"Snapshot description: {snapshot_description}")

    db_snapshot_controller.modify_snapshot(instance_id, snapshot_id, description="Test snapshot")
    print(f"Snapshot modified: {snapshot_id}")

    db_snapshot_controller.restore_snapshot(instance_id, snapshot_id)
    print(f"Restored from snapshot: {snapshot_id}")

    db_snapshot_controller.delete_snapshot(instance_id, snapshot_id)
    print(f"Snapshot deleted: {snapshot_id}")


    # Testing query execution
    print("\nTesting query execution:")
    create_table_query = "CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT)"
    db_instance_controller.service.execute_query(instance_id, create_table_query, "testdb")
    print("Table created")

    insert_query = "INSERT INTO test_table (name) VALUES ('Test Name')"
    db_instance_controller.service.execute_query(instance_id, insert_query, "testdb")
    print("Data inserted")

    select_query = "SELECT * FROM test_table"
    result = db_instance_controller.service.execute_query(instance_id, select_query, "testdb")
    print(f"SELECT query result: {result}")

    # Cleanup
    db_instance_controller.delete_db_instance(instance_id)
    print(f"DB instance deleted: {instance_id}")

    if os.path.exists(db_file):
        os.remove(db_file)
        print(f"Database file {db_file} deleted")

if __name__ == "__main__":
    main()