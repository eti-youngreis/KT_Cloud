from datetime import datetime
from DB.NEW_KT_DB.Controller.DBInstanceController import DBInstanceController
from DB.NEW_KT_DB.Controller.DBSnapshotController import DBSnapshotController
from DB.NEW_KT_DB.DataAccess.DBInstanceManager import DBInstanceManager
from DB.NEW_KT_DB.Service.Classes.DBInstanceService import DBInstanceService

# demonstrate all object functionallity

print('''---------------------Start Of session----------------------''')
start_integration_date_time = datetime.now()
print('''{start_integration_date_time} deonstration of object DBInstance start''')

db_instance_manager = DBInstanceManager("../DBs/mainDB.db")
db_instance_service = DBInstanceService(db_instance_manager)
db_instance_controller = DBInstanceController(db_instance_service)
db_snapshot_controller = DBSnapshotController(db_instance_service)

# create
start_creation_date_time = datetime.now()
print('''{start_creation_date_time} going to create db DBInstance names "example"''')

db_instance_controller.create_db_instance(db_instance_identifier='example', allocated_storage=20,
                                          master_user_name='admin', master_user_password="password123", db_name="exampledb")
end_creation_date_time = datetime.now()
print('''{end_creation_date_time} DBInstance "example" created successfully''')
print(f'Total creation time: {
      end_creation_date_time - start_creation_date_time}')

# describe
start_description_date_time = datetime.now()
print('''{start_description_date_time} going to describe db DBInstance "example"''')
description = db_instance_controller.describe_db_instance(
    db_instance_identifier='example')
print('''DB instance "example" description: {description}''')
end_description_date_time = datetime.now()
print('''{end_description_date_time} DBInstance "example" described successfully''')
print(f'Total description time: {
      end_description_date_time - start_description_date_time}')

# modify
start_modification_date_time = datetime.now()
print('''{start_modification_date_time} going to modify db DBInstance "example"''')
db_instance_controller.modify_db_instance(
    db_instance_identifier='example', allocated_storage=10)
end_modification_date_time = datetime.now()
print('''{end_modification_date_time} DBInstance "example" modified successfully''')
print(f'Total modification time: {
      end_modification_date_time - start_modification_date_time}')

# describe after modification
start_description_date_time = datetime.now()
print('''{start_description_date_time} going to describe db DBInstance "example" after modification''')
description = db_instance_controller.describe_db_instance(
    db_instance_identifier='example')
print(
    '''DB instance "example" description after modification: {description}''')
end_description_date_time = datetime.now()
print('''{end_description_date_time} DBInstance "example" described successfully''')
print(f'Total description time: {
      end_description_date_time - start_description_date_time}')

# create new database in DBInstance "example"
start_creation_database_date_time = datetime.now()
print('''{start_creation_database_date_time} going to create db "exampledb" in DBInstance "example"''')
db_instance_controller.execute_query(
    db_instance_identifier='example', query='CREATE DATABASE exampledb', db_name="exampledb")
end_creation_database_date_time = datetime.now()
print('''{end_creation_database_date_time} DBInstance "example" database "exampledb" created successfully''')
print(f'Total creation database time: {
      end_creation_database_date_time - start_creation_database_date_time}')

# create new table in DBInstance "example in exampledb"
start_creation_table_date_time = datetime.now()
print('''{start_creation_table_date_time} going to create table "example" in DBInstance "example" in database "exampledb"''')
db_instance_controller.execute_query(db_instance_identifier='example',
                                     query='CREATE TABLE example (id INTEGER PRIMARY KEY, name TEXT)', db_name="exampledb")
end_creation_table_date_time = datetime.now()
print('''{end_creation_table_date_time} DBInstance "example" table "example" created successfully''')
print(f'Total creation table time: {
      end_creation_table_date_time - start_creation_table_date_time}')

# insert data in DBInstance "example in exampledb"
start_insert_data_date_time = datetime.now()
print('''{start_insert_data_date_time} going to insert data in DBInstance "example" in database "exampledb"''')
db_instance_controller.execute_query(db_instance_identifier='example',
                                     query='INSERT INTO example (id, name) VALUES (1, "John"), (2, "Alice")', db_name="exampledb")
end_insert_data_date_time = datetime.now()
print('''{end_
      insert_data_date_time} DBInstance "example" data inserted successfully''')
print(f'Total insert data time: {
      end_insert_data_date_time - start_insert_data_date_time}')

# select data in DBInstance "example in exampledb"
start_select_data_date_time = datetime.now()
print('''{start_select_data_date_time} going to select data in DBInstance "example" in database "exampledb"''')
results = db_instance_controller.execute_query(
    db_instance_identifier='example', query='SELECT * FROM example', db_name="exampledb")
end_select_data_date_time = datetime.now()
print('''results of the select query: {results}''')
print('''{end_select_data_date_time} DBInstance "example" data selected successfully''')
print(f'Total select data time: {
      end_select_data_date_time - start_select_data_date_time}')

# create snapshot
start_creation_snapshot_date_time = datetime.now()
print('''{start_creation_snapshot_date_time} going to create db DBInstance snapshot "example"''')
db_snapshot_controller.create_snapshot('example', 'example_snapshot')
end_creation_snapshot_date_time = datetime.now()
print('''{end_
      creation_snapshot_date_time} DBInstance "example" snapshot created successfully''')
print(f'Total creation snapshot time: {
      end_creation_snapshot_date_time - start_creation_snapshot_date_time}')

# delete one record
start_delete_record_date_time = datetime.now()
print('''{start_delete_record_date_time} going to delete one record in DBInstance "example" in database "exampledb"''')
db_instance_controller.execute_query(db_instance_identifier='example',
                                     query='DELETE FROM example WHERE id = 1', db_name="exampledb")
end_delete_record_date_time = datetime.now()
print('''{end_delete_record_date_time} DBInstance "example" record deleted successfully''')
print(f'Total delete record time: {
      end_delete_record_date_time - start_delete_record_date_time}')


# select data in DBInstance "example in exampledb"
start_select_data_date_time = datetime.now()
print('''{start_select_data_date_time} going to select data in DBInstance "example" in database "exampledb"''')
results = db_instance_controller.execute_query(
    db_instance_identifier='example', query='SELECT * FROM example', db_name="exampledb")
end_select_data_date_time = datetime.now()
print('''results of the select query: {results}''')
print('''{end_select_data_date_time} DBInstance "example" data selected successfully''')
print(f'Total select data time: {
      end_select_data_date_time - start_select_data_date_time}')

# restore snapshot
start_restore_snapshot_date_time = datetime.now()
print('''{start_restore_snapshot_date_time} going to restore db DBInstance "example" from snapshot "example_snapshot_1"''')
db_snapshot_controller.restore_snapshot(
    'example', 'example_snapshot_1')
end_restore_snapshot_date_time = datetime.now()
print('''{end_restore_snapshot_date_
      time} DBInstance "example" restored from snapshot "example_snapshot_1" successfully''')
print(f'Total restore snapshot time: {
      end_restore_snapshot_date_time - start_restore_snapshot_date_time}')

# list snapshots
start_list_snapshots_date_time = datetime.now()
print('''{start_list_snapshots_date_time} going to list db DBInstance "example" snapshots''')
snapshots = db_snapshot_controller.list_snapshots('example')
print('''DB instance "example" snapshots: {snapshots}''')
end_list_snapshots_date_time = datetime.now()
print('''{end_list_snapshots_date_time} DBInstance "example" snapshots listed successfully''')
print(f'Total list snapshots time: {
      end_list_snapshots_date_time - start_list_snapshots_date_time}')

# delete snapshot
start_deletion_snapshot_date_time = datetime.now()
print('''{start_deletion_snapshot_date_time} going to delete db DBInstance "example" snapshot "example_snapshot_1"''')
db_snapshot_controller.delete_snapshot('example', 'example_snapshot_1')
end_deletion_snapshot_date_time = datetime.now()
print('''{end_
      deletion_snapshot_date_time} DBInstance "example" snapshot "example_snapshot_1" deleted successfully''')
print(f'Total deletion snapshot time: {
      end_deletion_snapshot_date_time - start_deletion_snapshot_date_time}')

# delete
start_deletion_date_time = datetime.now()
print('''{start_deletion_date_time} going to delete db DBInstance "example"''')
db_instance_controller.delete_db_instance(db_instance_identifier='example')
# print('{current_date_time} verify db DBInstance "example" deleted by checking if it exist')
# db_instance_controller.verify_deletion('example')
end_deletion_date_time = datetime.now()
print('''{end_deletion_date_time} DBInstance "example" deleted successfully''')
print(f'Total deletion time: {
      end_deletion_date_time - start_deletion_date_time}')

end_integration_date_time = datetime.now()
print('''{end_integration_date_time} deonstration of object DBInstance ended successfully''')
print('''---------------------End Of session----------------------''')
print(f'Total integration time: {
      end_integration_date_time - start_integration_date_time}')
