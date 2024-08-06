from rds_client import *
from try2.functions_in_configuration_table import *

def is_valid_json(json_string):
    try:
        json.loads(json_string)
        return True
    except json.JSONDecodeError:
        return False


def map_to_object(obj):
    class_name, obj_id, metadata, parent_id = obj
    if is_valid_json(metadata):
        metadata = json.loads(metadata)
    if class_name != "DBInstance" or metadata['status'] != 'stopped':
        wake_up_object(obj_id, class_name, metadata, parent_id)


start_object_management_table('object_management')
# del_class_from_management_table('DBSnapshot')
# del_class_from_management_table('DBInstance')

db_instances = get_objects_by_class_name('object_management', 'object_management', 'DBInstance')
db_snapshots = get_objects_by_class_name('object_management', 'object_management', 'DBSnapshot')
for obj in db_instances:
    map_to_object(obj)
for snap in db_snapshots:
    map_to_object(snap)
# create_events_table()
# insert_all_events()

# print(create_db_instance(db_instance_identifier='myDB', allocated_storage=123,
#                                 master_username='admin', master_user_password='ttt', db_name='countries'))
instance = get_instance_by_id('mostlastDB')
# instance.create_database('publishers')
# instance.create_database('categories')
db = instance.get_path('categories')
create_table(db, 'categories_table', 'id INTEGER PRIMARY KEY, name TEXT NOT NULL')
execute_query(db, '''INSERT INTO categories_table (id,name) VALUES (?, ?)''', (2024, "silver"))
print(execute_query(db, '''SELECT * FROM categories_table''', None))
print(execute_query('object_management', '''SELECT * FROM object_management''', None))
# print(stop_db_instance(db_instance_identifier='ourDB'))
# print(start_db_instance(db_instance_identifier='ourDB'))
# create_db_snapshot(db_instance_identifier='qweDB',
#                    db_snapshot_identifier='twentySnapshot')

# restore_db_instance_from_snapshot(db_instance_identifier='mostlastDB',
#                     db_snapshot_identifier='twentySnapshot')