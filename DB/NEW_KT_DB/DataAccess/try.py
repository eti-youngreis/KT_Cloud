from DBManager import DBManager
from ObjectManager import ObjectManager
import sqlite3


# Initialize the DBManager
db = DBManager("example.db")

# db.execute_query_without_results('CREATE TABLE IF NOT EXISTS MNG (object_id INTEGER PRIMARY KEY AUTOINCREMENT,type_object TEXT NOT NULL,metadata TEXT NOT NULL)')

# insert1 = '''INSERT INTO MNG (type_object, metadata) VALUES ('Person', '{"name": "Alice", "age": 30, "occupation": "Engineer"}');'''
# insert2 = '''INSERT INTO MNG (type_object, metadata) VALUES ('Person', '{"name": "Bob", "age": 25, "occupation": "Designer"}');'''
# insert3 = '''INSERT INTO MNG (type_object, metadata) VALUES ('Place', '{"name": "Central Park", "city": "New York", "area_acres": 843}');'''

# db.execute_query_without_results(insert1)
# db.execute_query_without_results(insert2)
# db.execute_query_without_results(insert3)

# print(db.execute_query_with_single_result('select * from MNG where object_id = 1'))

# print(db.execute_query_with_multiple_results('select * from MNG where object_id in (1,2)'))

# db.create_table('MNG1', 'object_id INTEGER PRIMARY KEY AUTOINCREMENT,type_object TEXT NOT NULL,metadata TEXT NOT NULL')

# values = ''' ('Person', '{"name": "Alice", "age": 30, "occupation": "Engineer"}');'''

# db.insert_data_into_table('MNG1', values, 'type_object, metadata')

# db.delete_data_from_table('MNG1', 'object_id= 1')

# print(db.execute_query_with_single_result('select * from MNG1 where object_id = 2'))

# print(db.get_column_names_of_table('MNG'))

# print(db.get_all_data_from_table('MNG2'))

# print(db.select_and_return_records_from_table('MNG', ['metadata']), 'object_id = 1')

# print(db.is_object_exist('MNG', 'object_id = 1'))

# print(db.describe_table('MNG'))

# print(db.describe_table('MNG'))


db_om = ObjectManager("example.db")

# connection = sqlite3.connect("example.db")

# c = connection.cursor()
# c.execute('select * from mng_NEWs')
# print(c.fetchall())

# db.get_all_data_from_table('mng_NEWs')

# db_om.create_management_table('NEW')

# print(db.describe_table('mng_NEWs'))

# db.execute_query_without_results('drop table mng_NEWs')

# db_om.save_in_memory('NEW', "('xxx', 'lkljl')", 'type_object, metadata')

# db_om.save_in_memory('NEW', "('yyy', 'lkljljh')", 'type_object, metadata')

# db_om.save_in_memory('NEW', "('zzz', 'lkljljh')", 'type_object, metadata')

# db_om.delete_from_memory_by_criteria('NEW', "type_object = 'xxx'")

# db_om.delete_from_memory_by_pk('NEW', 'object_id', '2')

# db_om.update_in_memory('NEW', "type_object = 'newzzz', metadata = 'ggg'", "object_id = 3")

# print(db.get_all_data_from_table('mng_NEWs'))

# print(db.get_data_from_table('mng_NEWs'))

# print(db_om.get_from_memory('NEW'))

# print(db_om.get_all_objects_from_memory('NEW'))

print(db_om.get_from_memory('NEW', 'object_id'))

print(db_om.get_from_memory('NEW', '*', 'object_id = 4'))

