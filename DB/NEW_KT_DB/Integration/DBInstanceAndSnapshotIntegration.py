import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))
from datetime import datetime
from DB.NEW_KT_DB.Controller.DBInstanceController import DBInstanceController
from DB.NEW_KT_DB.Controller.DBSnapshotController import DBSnapshotController
from DB.NEW_KT_DB.DataAccess.DBInstanceManager import DBInstanceManager
from DB.NEW_KT_DB.Service.Classes.DBInstanceService import DBInstanceService
import time


# Add these constants at the top of your file
PINK = '\033[95m'
RESET = '\033[0m'
BLUE = '\033[94m'
GREEN = '\033[92m'


print(f"{BLUE}---------------------Start Of session----------------------{RESET}")

db_path = os.path.join(os.path.dirname(__file__), '..', 'DBs', 'mainDB.db')
db_instance_manager = DBInstanceManager(db_path)
db_instance_service = DBInstanceService(db_instance_manager)
db_instance_controller = DBInstanceController(db_instance_service)
db_snapshot_controller = DBSnapshotController(db_instance_service)

print(f"\n{BLUE}---------------------Create DBInstance----------------------{RESET}")
start_creation_date_time = datetime.now()
print(f"{PINK}{start_creation_date_time} going to create db DBInstance names 'exampleDbInstance'{RESET}")

db_instance_controller.create_db_instance(db_instance_identifier='exampleDbInstance', allocated_storage=20,
           master_user_name='admin', master_user_password="password123")
end_creation_date_time = datetime.now()
print(f"{GREEN}{end_creation_date_time} DBInstance 'exampleDbInstance' created successfully{RESET}")
print(f"{PINK}Total creation time: {end_creation_date_time - start_creation_date_time}{RESET}")
time.sleep(2)

print(f"\n{BLUE}---------------------Describe DBInstance----------------------{RESET}")
start_description_date_time = datetime.now()
print(f"{PINK}{start_description_date_time} going to describe db DBInstance 'exampleDbInstance'{RESET}")
description = db_instance_controller.describe_db_instance(db_instance_identifier='exampleDbInstance')
print(f"\nDB instance 'exampleDbInstance' description: {description}\n")
end_description_date_time = datetime.now()
print(f"{GREEN}{end_description_date_time} DBInstance 'exampleDbInstance' described successfully{RESET}")
print(f"{PINK}Total description time: {end_description_date_time - start_description_date_time}{RESET}")
time.sleep(2)

print(f"\n{BLUE}---------------------Modify DBInstance----------------------{RESET}")
start_modification_date_time = datetime.now()
print(f"{PINK}{start_modification_date_time} going to modify db DBInstance 'exampleDbInstance'{RESET}")
db_instance_controller.modify_db_instance(db_instance_identifier='exampleDbInstance', allocated_storage=10)
end_modification_date_time = datetime.now()
print(f"{GREEN}{end_modification_date_time} DBInstance 'exampleDbInstance' modified successfully{RESET}")
print(f"{PINK}Total modification time: {end_modification_date_time - start_modification_date_time}{RESET}")
time.sleep(2)

print(f"\n{BLUE}---------------------Describe Modified DBInstance----------------------{RESET}")
start_description_date_time = datetime.now()
print(f"{PINK}{start_description_date_time} going to describe db DBInstance 'exampleDbInstance' after modification{RESET}")
description = db_instance_controller.describe_db_instance(db_instance_identifier='exampleDbInstance')
print(f"\nDB instance 'exampleDbInstance' description after modification: {description}\n")
end_description_date_time = datetime.now()
print(f"{GREEN}{end_description_date_time} DBInstance 'exampleDbInstance' described successfully{RESET}")
print(f"{PINK}Total description time: {end_description_date_time - start_description_date_time}{RESET}")
time.sleep(2)

print(f"\n{BLUE}---------------------Create Database----------------------{RESET}")
start_creation_database_date_time = datetime.now()
print(f"{PINK}{start_creation_database_date_time} going to create db 'exampledb' in DBInstance 'exampleDbInstance'{RESET}")
db_instance_controller.execute_query(db_instance_identifier='exampleDbInstance', query='CREATE DATABASE exampledb', db_name="exampledb")
end_creation_database_date_time = datetime.now()
print(f"{GREEN}{end_creation_database_date_time} DBInstance 'exampleDbInstance' database 'exampledb' created successfully{RESET}")
print(f"{PINK}Total creation database time: {end_creation_database_date_time - start_creation_database_date_time}{RESET}")
time.sleep(2)

print(f"\n{BLUE}---------------------Create Table----------------------{RESET}")
start_creation_table_date_time = datetime.now()
print(f"{PINK}{start_creation_table_date_time} going to create table 'exampleTable' in DBInstance 'exampleDbInstance' in database 'exampledb'{RESET}")
db_instance_controller.execute_query(db_instance_identifier='exampleDbInstance',
      query='CREATE TABLE exampleTable (id INTEGER PRIMARY KEY, name TEXT)', db_name="exampledb")
end_creation_table_date_time = datetime.now()
print(f"{GREEN}{end_creation_table_date_time} DBInstance 'exampleDbInstance' table 'exampleTable' created successfully{RESET}")
print(f"{PINK}Total creation table time: {end_creation_table_date_time - start_creation_table_date_time}{RESET}")
time.sleep(2)

print(f"\n{BLUE}---------------------Insert Data----------------------{RESET}")
start_insert_data_date_time = datetime.now()
print(f"{PINK}{start_insert_data_date_time} going to insert data in DBInstance 'exampleDbInstance' in database 'exampledb'{RESET}")
db_instance_controller.execute_query(db_instance_identifier='exampleDbInstance',
      query='INSERT INTO exampleTable (id, name) VALUES (1, "John"), (2, "Alice")', db_name="exampledb")
end_insert_data_date_time = datetime.now()
print(f"{GREEN}{end_insert_data_date_time} DBInstance 'exampleDbInstance' data inserted successfully{RESET}")
print(f"{PINK}Total insert data time: {end_insert_data_date_time - start_insert_data_date_time}{RESET}")
time.sleep(2)

print(f"\n{BLUE}---------------------Select Data----------------------{RESET}")
start_select_data_date_time = datetime.now()
print(f"{PINK}{start_select_data_date_time} going to select data in DBInstance 'exampleDbInstance' in database 'exampledb'{RESET}")
results = db_instance_controller.execute_query(db_instance_identifier='exampleDbInstance', query='SELECT * FROM exampleTable', db_name="exampledb")
end_select_data_date_time = datetime.now()
print(f"\nresults of the select query: {results}\n")
print(f"{GREEN}{end_select_data_date_time} DBInstance 'exampleDbInstance' data selected successfully{RESET}")
print(f"{PINK}Total select data time: {end_select_data_date_time - start_select_data_date_time}{RESET}")
time.sleep(2)

print(f"\n{BLUE}---------------------Create Snapshot----------------------{RESET}")
start_creation_snapshot_date_time = datetime.now()
print(f"{PINK}{start_creation_snapshot_date_time} going to create db DBInstance snapshot 'example_snapshot_1'{RESET}")
db_snapshot_controller.create_snapshot('exampleDbInstance', 'example_snapshot_1')
end_creation_snapshot_date_time = datetime.now()
print(f"{GREEN}{end_creation_snapshot_date_time} DBInstance 'exampleDbInstance' snapshot created successfully{RESET}")
print(f"{PINK}Total creation snapshot time: {end_creation_snapshot_date_time - start_creation_snapshot_date_time}{RESET}")
time.sleep(2)

print(f"\n{BLUE}---------------------Delete Record----------------------{RESET}")
start_delete_record_date_time = datetime.now()
print(f"{PINK}{start_delete_record_date_time} going to delete one record in DBInstance 'exampleDbInstance' in database 'exampledb'{RESET}")
db_instance_controller.execute_query(db_instance_identifier='exampleDbInstance',
      query='DELETE FROM exampleTable WHERE id = 1', db_name="exampledb")
end_delete_record_date_time = datetime.now()
print(f"{GREEN}{end_delete_record_date_time} in DBInstance 'exampleDbInstance' record deleted successfully{RESET}")
print(f"{PINK}Total delete record time: {end_delete_record_date_time - start_delete_record_date_time}{RESET}")
time.sleep(2)

print(f"\n{BLUE}---------------------Select After Delete----------------------{RESET}")
start_select_data_date_time = datetime.now()
print(f"{PINK}{start_select_data_date_time} Check that the data has been successfully deleted in table exampleTable in Snapshot example_snapshot_1{RESET}")
results = db_instance_controller.execute_query(db_instance_identifier='exampleDbInstance', query='SELECT * FROM exampleTable', db_name="exampledb")
end_select_data_date_time = datetime.now()
print(f"\nresults of the select query: {results}\n")
print(f"{GREEN}{end_select_data_date_time} DBInstance 'exampleDbInstance' data selected successfully{RESET}")
print(f"{PINK}Total select data time: {end_select_data_date_time - start_select_data_date_time}{RESET}")
time.sleep(2)

print(f"\n{BLUE}---------------------Create Another Snapshot----------------------{RESET}")
start_creation_snapshot_date_time = datetime.now()
print(f"{PINK}{start_creation_snapshot_date_time} going to create db DBInstance snapshot 'example_snapshot_2'{RESET}")
db_snapshot_controller.create_snapshot('exampleDbInstance', 'example_snapshot_2')
end_creation_snapshot_date_time = datetime.now()
print(f"{GREEN}{end_creation_snapshot_date_time} DBInstance 'exampleDbInstance' snapshot created successfully{RESET}")
print(f"{PINK}Total creation snapshot time: {end_creation_snapshot_date_time - start_creation_snapshot_date_time}{RESET}")
time.sleep(2)

print(f"\n{BLUE}---------------------Select Data Again----------------------{RESET}")
start_select_data_date_time = datetime.now()
print(f"{PINK}{start_select_data_date_time} going to select data in DBInstance 'exampleDbInstance' in database 'exampledb'{RESET}")
results = db_instance_controller.execute_query(db_instance_identifier='exampleDbInstance', query='SELECT * FROM exampleTable', db_name="exampledb")
end_select_data_date_time = datetime.now()
print(f"\nresults of the select query: {results}\n")
print(f"{GREEN}{end_select_data_date_time} DBInstance 'exampleDbInstance' data selected successfully{RESET}")
print(f"{PINK}Total select data time: {end_select_data_date_time - start_select_data_date_time}{RESET}")
time.sleep(2)

print(f"\n{BLUE}---------------------Restore Snapshot----------------------{RESET}")
start_restore_snapshot_date_time = datetime.now()
print(f"{PINK}{start_restore_snapshot_date_time} going to restore db DBInstance 'exampleDbInstance' from snapshot 'example_snapshot_1'{RESET}")
db_snapshot_controller.restore_snapshot('exampleDbInstance', 'example_snapshot_1')
end_restore_snapshot_date_time = datetime.now()
print(f"{GREEN}{end_restore_snapshot_date_time} DBInstance 'exampleDbInstance' restored from snapshot 'example_snapshot_1' successfully{RESET}")
print(f"{PINK}Total restore snapshot time: {end_restore_snapshot_date_time - start_restore_snapshot_date_time}{RESET}")
time.sleep(2)

print(f"\n{BLUE}---------------------Select After Restore----------------------{RESET}")
start_select_data_date_time = datetime.now()
print(f"{PINK}{start_select_data_date_time} Checking that the data exists in the Snapshot to which we restored, Snapshot example_snapshot_1{RESET}")
results = db_instance_controller.execute_query(db_instance_identifier='exampleDbInstance', query='SELECT * FROM exampleTable', db_name="exampledb")
end_select_data_date_time = datetime.now()
print(f"\nresults of the select query: {results}\n")
print(f"{GREEN}{end_select_data_date_time} DBInstance 'exampleDbInstance' data selected successfully{RESET}")
print(f"{PINK}Total select data time: {end_select_data_date_time - start_select_data_date_time}{RESET}")
time.sleep(2)

print(f"\n{BLUE}---------------------Delete Snapshot----------------------{RESET}")
start_deletion_snapshot_date_time = datetime.now()
print(f"{PINK}{start_deletion_snapshot_date_time} going to delete db DBInstance snapshot 'example_snapshot_2'{RESET}")
db_snapshot_controller.delete_snapshot('exampleDbInstance', 'example_snapshot_2')
end_creation_snapshot_date_time = datetime.now()
print(f"{GREEN}{end_creation_snapshot_date_time} DBInstance 'exampleDbInstance' snapshot deleted successfully{RESET}")
print(f"{PINK}Total deletion snapshot time: {end_creation_snapshot_date_time - start_deletion_snapshot_date_time}{RESET}")
time.sleep(2)

print(f"\n{BLUE}---------------------List Snapshots----------------------{RESET}")
start_list_snapshots_date_time = datetime.now()
print(f"{PINK}{start_list_snapshots_date_time} going to list db DBInstance 'exampleDbInstance' snapshots{RESET}")
snapshots = db_snapshot_controller.list_snapshots('exampleDbInstance')
print(f"\nDB instance 'exampleDbInstance' snapshots: {snapshots}\n")
end_list_snapshots_date_time = datetime.now()
print(f"{GREEN}{end_list_snapshots_date_time} DBInstance 'exampleDbInstance' snapshots listed successfully{RESET}")
print(f"{PINK}Total list snapshots time: {end_list_snapshots_date_time - start_list_snapshots_date_time}{RESET}")
time.sleep(2)

print(f"\n{BLUE}---------------------Delete Another Snapshot----------------------{RESET}")
start_deletion_snapshot_date_time = datetime.now()
print(f"{PINK}{start_deletion_snapshot_date_time} going to delete db DBInstance 'exampleDbInstance' snapshot 'example_snapshot_1'{RESET}")
db_snapshot_controller.delete_snapshot('exampleDbInstance', 'example_snapshot_1')
end_deletion_snapshot_date_time = datetime.now()
print(f"{GREEN}{end_deletion_snapshot_date_time} DBInstance 'exampleDbInstance' snapshot 'example_snapshot_1' deleted successfully{RESET}")
print(f"{PINK}Total deletion snapshot time: {end_deletion_snapshot_date_time - start_deletion_snapshot_date_time}{RESET}")
time.sleep(2)

print(f"\n{BLUE}---------------------Delete DBInstance----------------------{RESET}")
start_deletion_date_time = datetime.now()
print(f"{PINK}{start_deletion_date_time} going to delete db DBInstance 'exampleDbInstance'{RESET}")
db_instance_controller.delete_db_instance(db_instance_identifier='exampleDbInstance')
end_deletion_date_time = datetime.now()
print(f"{GREEN}{end_deletion_date_time} DBInstance 'exampleDbInstance' deleted successfully{RESET}")
print(f"{PINK}Total deletion time: {end_deletion_date_time - start_deletion_date_time}{RESET}")
time.sleep(2)

print(f"\n{BLUE}---------------------Attempt to Get Deleted DBInstance----------------------{RESET}")
start_get_deleted_instance_time = datetime.now()
print(f"{PINK}{start_get_deleted_instance_time} Attempting to get deleted DBInstance 'exampleDbInstance'{RESET}\n")
try:
    deleted_instance = db_instance_controller.get_db_instance(db_instance_identifier='exampleDbInstance')
    print("Error: DBInstance 'exampleDbInstance' still exists after deletion")
except Exception as e:
    print(f"Success: Unable to get deleted DBInstance 'exampleDbInstance'. Error: {str(e)}")
end_get_deleted_instance_time = datetime.now()
print(f"\n{PINK}Total attempt time: {end_get_deleted_instance_time - start_get_deleted_instance_time}{RESET}")
time.sleep(2)

print(f"\n{BLUE}---------------------End Of session----------------------{RESET}")