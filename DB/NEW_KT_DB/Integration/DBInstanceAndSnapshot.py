import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))
from datetime import datetime
from DB.NEW_KT_DB.Controller.DBInstanceController import DBInstanceController
from DB.NEW_KT_DB.Controller.DBSnapshotController import DBSnapshotController
from DB.NEW_KT_DB.DataAccess.DBInstanceManager import DBInstanceManager
from DB.NEW_KT_DB.Service.Classes.DBInstanceService import DBInstanceService

print("---------------------Start Of session----------------------")
start_integration_date_time = datetime.now()
print(f"{start_integration_date_time} demonstration of object DBInstance start")

db_path = os.path.join(os.path.dirname(__file__), '..', 'DBs', 'mainDB.db')
db_instance_manager = DBInstanceManager(db_path)
db_instance_service = DBInstanceService(db_instance_manager)
db_instance_controller = DBInstanceController(db_instance_service)
db_snapshot_controller = DBSnapshotController(db_instance_service)

print("---------------------Create DBInstance----------------------")
start_creation_date_time = datetime.now()
print(f"{start_creation_date_time} going to create db DBInstance names 'example'")

db_instance_controller.create_db_instance(db_instance_identifier='example', allocated_storage=20,
           master_user_name='admin', master_user_password="password123")
end_creation_date_time = datetime.now()
print(f"{end_creation_date_time} DBInstance 'example' created successfully")
print(f"Total creation time: {end_creation_date_time - start_creation_date_time}")

print("---------------------Describe DBInstance----------------------")
start_description_date_time = datetime.now()
print(f"{start_description_date_time} going to describe db DBInstance 'example'")
description = db_instance_controller.describe_db_instance(db_instance_identifier='example')
print(f"DB instance 'example' description: {description}")
end_description_date_time = datetime.now()
print(f"{end_description_date_time} DBInstance 'example' described successfully")
print(f"Total description time: {end_description_date_time - start_description_date_time}")

print("---------------------Modify DBInstance----------------------")
start_modification_date_time = datetime.now()
print(f"{start_modification_date_time} going to modify db DBInstance 'example'")
db_instance_controller.modify_db_instance(db_instance_identifier='example', allocated_storage=10)
end_modification_date_time = datetime.now()
print(f"{end_modification_date_time} DBInstance 'example' modified successfully")
print(f"Total modification time: {end_modification_date_time - start_modification_date_time}")

print("---------------------Describe Modified DBInstance----------------------")
start_description_date_time = datetime.now()
print(f"{start_description_date_time} going to describe db DBInstance 'example' after modification")
description = db_instance_controller.describe_db_instance(db_instance_identifier='example')
print(f"DB instance 'example' description after modification: {description}")
end_description_date_time = datetime.now()
print(f"{end_description_date_time} DBInstance 'example' described successfully")
print(f"Total description time: {end_description_date_time - start_description_date_time}")

print("---------------------Create Database----------------------")
start_creation_database_date_time = datetime.now()
print(f"{start_creation_database_date_time} going to create db 'exampledb' in DBInstance 'example'")
db_instance_controller.execute_query(db_instance_identifier='example', query='CREATE DATABASE exampledb', db_name="exampledb")
end_creation_database_date_time = datetime.now()
print(f"{end_creation_database_date_time} DBInstance 'example' database 'exampledb' created successfully")
print(f"Total creation database time: {end_creation_database_date_time - start_creation_database_date_time}")

print("---------------------Create Table----------------------")
start_creation_table_date_time = datetime.now()
print(f"{start_creation_table_date_time} going to create table 'example' in DBInstance 'example' in database 'exampledb'")
db_instance_controller.execute_query(db_instance_identifier='example',
      query='CREATE TABLE example (id INTEGER PRIMARY KEY, name TEXT)', db_name="exampledb")
end_creation_table_date_time = datetime.now()
print(f"{end_creation_table_date_time} DBInstance 'example' table 'example' created successfully")
print(f"Total creation table time: {end_creation_table_date_time - start_creation_table_date_time}")

print("---------------------Insert Data----------------------")
start_insert_data_date_time = datetime.now()
print(f"{start_insert_data_date_time} going to insert data in DBInstance 'example' in database 'exampledb'")
db_instance_controller.execute_query(db_instance_identifier='example',
      query='INSERT INTO example (id, name) VALUES (1, "John"), (2, "Alice")', db_name="exampledb")
end_insert_data_date_time = datetime.now()
print(f"{end_insert_data_date_time} DBInstance 'example' data inserted successfully")
print(f"Total insert data time: {end_insert_data_date_time - start_insert_data_date_time}")

print("---------------------Select Data----------------------")
start_select_data_date_time = datetime.now()
print(f"{start_select_data_date_time} going to select data in DBInstance 'example' in database 'exampledb'")
results = db_instance_controller.execute_query(db_instance_identifier='example', query='SELECT * FROM example', db_name="exampledb")
end_select_data_date_time = datetime.now()
print(f"results of the select query: {results}")
print(f"{end_select_data_date_time} DBInstance 'example' data selected successfully")
print(f"Total select data time: {end_select_data_date_time - start_select_data_date_time}")

print("---------------------Create Snapshot----------------------")
start_creation_snapshot_date_time = datetime.now()
print(f"{start_creation_snapshot_date_time} going to create db DBInstance snapshot 'example_snapshot_1'")
db_snapshot_controller.create_snapshot('example', 'example_snapshot_1')
end_creation_snapshot_date_time = datetime.now()
print(f"{end_creation_snapshot_date_time} DBInstance 'example' snapshot created successfully")
print(f"Total creation snapshot time: {end_creation_snapshot_date_time - start_creation_snapshot_date_time}")

print("---------------------Delete Record----------------------")
start_delete_record_date_time = datetime.now()
print(f"{start_delete_record_date_time} going to delete one record in DBInstance 'example' in database 'exampledb'")
db_instance_controller.execute_query(db_instance_identifier='example',
      query='DELETE FROM example WHERE id = 1', db_name="exampledb")
end_delete_record_date_time = datetime.now()
print(f"{end_delete_record_date_time} in DBInstance 'example' record deleted successfully")
print(f"Total delete record time: {end_delete_record_date_time - start_delete_record_date_time}")

print("---------------------Select After Delete----------------------")
start_select_data_date_time = datetime.now()
print(f"{start_select_data_date_time} Check that the data has been successfully deleted in table example in Snapshot example_snapshot_1")
results = db_instance_controller.execute_query(db_instance_identifier='example', query='SELECT * FROM example', db_name="exampledb")
end_select_data_date_time = datetime.now()
print(f"results of the select query: {results}")
print(f"{end_select_data_date_time} DBInstance 'example' data selected successfully")
print(f"Total select data time: {end_select_data_date_time - start_select_data_date_time}")

print("---------------------Create Another Snapshot----------------------")
start_creation_snapshot_date_time = datetime.now()
print(f"{start_creation_snapshot_date_time} going to create db DBInstance snapshot 'example_snapshot_2'")
db_snapshot_controller.create_snapshot('example', 'example_snapshot_2')
end_creation_snapshot_date_time = datetime.now()
print(f"{end_creation_snapshot_date_time} DBInstance 'example' snapshot created successfully")
print(f"Total creation snapshot time: {end_creation_snapshot_date_time - start_creation_snapshot_date_time}")

print("---------------------Select Data Again----------------------")
start_select_data_date_time = datetime.now()
print(f"{start_select_data_date_time} going to select data in DBInstance 'example' in database 'exampledb'")
results = db_instance_controller.execute_query(db_instance_identifier='example', query='SELECT * FROM example', db_name="exampledb")
end_select_data_date_time = datetime.now()
print(f"results of the select query: {results}")
print(f"{end_select_data_date_time} DBInstance 'example' data selected successfully")
print(f"Total select data time: {end_select_data_date_time - start_select_data_date_time}")

print("---------------------Restore Snapshot----------------------")
start_restore_snapshot_date_time = datetime.now()
print(f"{start_restore_snapshot_date_time} going to restore db DBInstance 'example' from snapshot 'example_snapshot_1'")
db_snapshot_controller.restore_snapshot('example', 'example_snapshot_1')
end_restore_snapshot_date_time = datetime.now()
print(f"{end_restore_snapshot_date_time} DBInstance 'example' restored from snapshot 'example_snapshot_1' successfully")
print(f"Total restore snapshot time: {end_restore_snapshot_date_time - start_restore_snapshot_date_time}")

print("---------------------Select After Restore----------------------")
start_select_data_date_time = datetime.now()
print(f"{start_select_data_date_time} Checking that the data exists in the Snapshot to which we restored, Snapshot example_snapshot_1")
results = db_instance_controller.execute_query(db_instance_identifier='example', query='SELECT * FROM example', db_name="exampledb")
end_select_data_date_time = datetime.now()
print(f"results of the select query: {results}")
print(f"{end_select_data_date_time} DBInstance 'example' data selected successfully")
print(f"Total select data time: {end_select_data_date_time - start_select_data_date_time}")

print("---------------------Delete Snapshot----------------------")
start_deletion_snapshot_date_time = datetime.now()
print(f"{start_deletion_snapshot_date_time} going to delete db DBInstance snapshot 'example_snapshot_2'")
db_snapshot_controller.delete_snapshot('example', 'example_snapshot_2')
end_creation_snapshot_date_time = datetime.now()
print(f"{end_creation_snapshot_date_time} DBInstance 'example' snapshot deleted successfully")
print(f"Total deletion snapshot time: {end_creation_snapshot_date_time - start_deletion_snapshot_date_time}")

print("---------------------List Snapshots----------------------")
start_list_snapshots_date_time = datetime.now()
print(f"{start_list_snapshots_date_time} going to list db DBInstance 'example' snapshots")
snapshots = db_snapshot_controller.list_snapshots('example')
print(f"DB instance 'example' snapshots: {snapshots}")
end_list_snapshots_date_time = datetime.now()
print(f"{end_list_snapshots_date_time} DBInstance 'example' snapshots listed successfully")
print(f"Total list snapshots time: {end_list_snapshots_date_time - start_list_snapshots_date_time}")

print("---------------------Delete Another Snapshot----------------------")
start_deletion_snapshot_date_time = datetime.now()
print(f"{start_deletion_snapshot_date_time} going to delete db DBInstance 'example' snapshot 'example_snapshot_1'")
db_snapshot_controller.delete_snapshot('example', 'example_snapshot_1')
end_deletion_snapshot_date_time = datetime.now()
print(f"{end_deletion_snapshot_date_time} DBInstance 'example' snapshot 'example_snapshot_1' deleted successfully")
print(f"Total deletion snapshot time: {end_deletion_snapshot_date_time - start_deletion_snapshot_date_time}")

print("---------------------Delete DBInstance----------------------")
start_deletion_date_time = datetime.now()
print(f"{start_deletion_date_time} going to delete db DBInstance 'example'")
db_instance_controller.delete_db_instance(db_instance_identifier='example')
end_deletion_date_time = datetime.now()
print(f"{end_deletion_date_time} DBInstance 'example' deleted successfully")
print(f"Total deletion time: {end_deletion_date_time - start_deletion_date_time}")

print("---------------------Attempt to Get Deleted DBInstance----------------------")
start_get_deleted_instance_time = datetime.now()
print(f"{start_get_deleted_instance_time} Attempting to get deleted DBInstance 'example'")
try:
    deleted_instance = db_instance_controller.describe_db_instance(db_instance_identifier='example')
    print("Error: DBInstance 'example' still exists after deletion")
except Exception as e:
    print(f"Success: Unable to get deleted DBInstance 'example'. Error: {str(e)}")
end_get_deleted_instance_time = datetime.now()
print(f"Total attempt time: {end_get_deleted_instance_time - start_get_deleted_instance_time}")

print("---------------------End Of session----------------------")
end_integration_date_time = datetime.now()
print(f"{end_integration_date_time} demonstration of object DBInstance ended successfully")
print(f"Total integration time: {end_integration_date_time - start_integration_date_time}")
