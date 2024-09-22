import sys
import os


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from Controller.DBSubnetGroupController import DBSubnetGroupController
from Service.Classes.DBSubnetGroupService import DBSubnetGroupService
from DataAccess.DBSubnetGroupManager import DBSubnetGroupManager
from DataAccess.ObjectManager import ObjectManager
from Storage.NEW_KT_Storage.DataAccess.StorageManager import StorageManager
from datetime import datetime

object_manager = ObjectManager('../../object_management_db.db')
storage_manager = StorageManager('../../s3')
subnet_group_manager = DBSubnetGroupManager(object_manager)
subnet_group_service = DBSubnetGroupService(subnet_group_manager, storage_manager)
subnet_group_controller = DBSubnetGroupController(subnet_group_service)


print('''---------------------Start Of session----------------------''')
start_time = datetime.now()
print(start_time)

print(f'''{datetime.now()} demonstration of DBSubnetGroups''')

# create
print('''___________________________________________________________''')
print(f'''{datetime.now()} creating a DBSubnetGroup with name: \'subnet_group_1\'''')
subnet_group_controller.create_db_subnet_group(
        db_subnet_group_name="subnet_group_1",
        subnets=[{"subnet_id": "subnet-12345678"}, {"subnet_id": "subnet-87654321"}],
        db_subnet_group_description="Test subnet group",
        vpc_id="vpc-12345678",
        db_subnet_group_arn="arn:aws:rds:us-west-2:123456789012:subgrp:subnet_group_1")

print("Creation of \'subnet_group_1\' passed with now exceptions")
print(datetime.now()-start_time)
print('''___________________________________________________________''')


# get
start_time_get = datetime.now()
print('''___________________________________________________________''')
print(f'''{start_time} retrieving \'subnet_group_1\'''')
subnet_group_1 = subnet_group_controller.get_db_subnet_group('subnet_group_1')
print(subnet_group_1.to_dict())

print(f'''{datetime.now()} retrieving \'subnet_group_1\' passed with now exceptions, valid object returned''')
print(datetime.now() - start_time_get)
print('''___________________________________________________________''')

# list
start_time_list = datetime.now()
print('''___________________________________________________________''')
print(f'''{start_time} listing all DBSubnetGroups''')
subnet_groups = subnet_group_controller.list_db_subnet_groups()
for subnet_group in subnet_groups:
    print(subnet_group.to_dict())

print(f'''{datetime.now()} listing all DBSubnetGroups passed with no exceptions''')
print(datetime.now() - start_time_list)
print('''___________________________________________________________''')

# update
start_time_update = datetime.now()
print('''___________________________________________________________''')
print(f'''{start_time} updating \'subnet_group_1\'''')
updated_subnet_group = subnet_group_controller.modify_db_subnet_group(
    name="subnet_group_1",
    subnets=[{"subnet_id": "subnet-11111111"}, {"subnet_id": "subnet-22222222"}],
    db_subnet_group_description="Updated test subnet group"
)

print(updated_subnet_group.to_dict())

print(f'''{datetime.now()} updating \'subnet_group_1\' passed with no exceptions''')
print(datetime.now() - start_time_update)
print('''___________________________________________________________''')

# delete
start_time_delete = datetime.now()
print('''___________________________________________________________''')
print(f'''{start_time} deleting \'subnet_group_1\'''')
subnet_group_controller.delete_db_subnet_group('subnet_group_1')

print(f'''{datetime.now()} deleting \'subnet_group_1\' passed with no exceptions''')
print(datetime.now() - start_time_delete)
print('''___________________________________________________________''')


print(f'''{datetime.now()} demonstration of DBSubnetGroup ended successfully''')
print(datetime.now())
print('''---------------------End Of session----------------------''')
print(datetime.now()-start_time)

