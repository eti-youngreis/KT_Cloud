import sys
import os
import time
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from Controller.DBSubnetGroupController import DBSubnetGroupController
from Service.Classes.DBSubnetGroupService import DBSubnetGroupService
from DataAccess.DBSubnetGroupManager import DBSubnetGroupManager
from DataAccess.ObjectManager import ObjectManager
from Storage.NEW_KT_Storage.DataAccess.StorageManager import StorageManager
from datetime import datetime
from Models.Subnet import Subnet

object_manager = ObjectManager("../DBs/mainDB.db")
storage_manager = StorageManager("../../s3")
subnet_group_manager = DBSubnetGroupManager(object_manager)
subnet_group_service = DBSubnetGroupService(subnet_group_manager, storage_manager)
subnet_group_controller = DBSubnetGroupController(subnet_group_service)


print("===================== Start Of DBSubnetGroup Demonstration =====================")
start_time = datetime.now()
print(f"Demonstration started at: {start_time}")

print(f"\n{datetime.now()} - Demonstrating CRUD operations and load balancing for DBSubnetGroups")




# CREATE OPERATION
print("\n========== CREATE OPERATION ==========")
print(f"{datetime.now()} - Creating a DBSubnetGroup with name: 'subnet_group_1'")
subnet_group_controller.create_db_subnet_group(
    db_subnet_group_name="subnet_group_1",
    subnets=[
        Subnet(subnet_id="subnet-12345678", ip_range="10.0.1.0/24"),
        Subnet(subnet_id="subnet-87654321", ip_range="10.0.2.0/24"),
    ],
    db_subnet_group_description="Test subnet group",
    vpc_id="vpc-12345678",
    db_subnet_group_arn="arn:aws:rds:us-west-2:123456789012:subgrp:subnet_group_1",
)

print("Creation of 'subnet_group_1' completed successfully")
print(f"Time taken for creation: {datetime.now() - start_time}")
time.sleep(4)

# READ OPERATION
start_time_get = datetime.now()
print("\n========== READ OPERATION ==========")
print(f"{start_time_get} - Retrieving 'subnet_group_1'")
subnet_group_1 = subnet_group_controller.get_db_subnet_group("subnet_group_1")
print("Retrieved subnet group details:")
print(subnet_group_1.to_dict())

print(f"{datetime.now()} - Retrieval of 'subnet_group_1' completed successfully")
print(f"Time taken for retrieval: {datetime.now() - start_time_get}")
time.sleep(4)

# LIST OPERATION
start_time_list = datetime.now()
print("\n========== LIST OPERATION ==========")
print(f"{start_time_list} - Listing all DBSubnetGroups")
subnet_groups = subnet_group_controller.list_db_subnet_groups()
print("All DBSubnetGroups:")
for subnet_group in subnet_groups:
    print(subnet_group.to_dict())

print(f"{datetime.now()} - Listing of all DBSubnetGroups completed successfully")
print(f"Time taken for listing: {datetime.now() - start_time_list}")
time.sleep(4)

# UPDATE OPERATION
start_time_update = datetime.now()
print("\n========== UPDATE OPERATION ==========")
print(f"{start_time_update} - Updating 'subnet_group_1'")
updated_subnet_group = subnet_group_controller.modify_db_subnet_group(
    name="subnet_group_1",
    subnets=[
        Subnet(subnet_id="subnet-11111111", ip_range="10.0.3.0/24"),
        Subnet(subnet_id="subnet-22222222", ip_range="10.0.4.0/24"),
    ],
    db_subnet_group_description="Updated test subnet group",
)

print("Updated subnet group details:")
print(updated_subnet_group.to_dict())
time.sleep(4)

print(f"{datetime.now()} - Updating 'subnet_group_1' completed successfully")
print(f"Time taken for update: {datetime.now() - start_time_update}")

# DELETE OPERATION
start_time_delete = datetime.now()
print("\n========== DELETE OPERATION ==========")
print(f"{start_time_delete} - Deleting 'subnet_group_1'")
subnet_group_controller.delete_db_subnet_group("subnet_group_1")

print(f"{datetime.now()} - Deletion of 'subnet_group_1' completed successfully")
print(f"Time taken for deletion: {datetime.now() - start_time_delete}")
time.sleep(4)

# LOAD BALANCING DEMONSTRATION
print("\n========== LOAD BALANCING DEMONSTRATION ==========")
print(f"{datetime.now()} - Demonstrating load balancing capability")

print("Creating a new subnet group for load balancing demonstration")
subnet_group_controller.create_db_subnet_group(
    db_subnet_group_name="load_balance_group",
    subnets=[
        Subnet(subnet_id="subnet-1", ip_range="10.0.1.0/24", availability_zone="us-west-2a"),
        Subnet(subnet_id="subnet-2", ip_range="10.0.2.0/24", availability_zone="us-west-2b")
    ],
    db_subnet_group_description="Load balancing test group",
    vpc_id="vpc-12345678"
)

print("Adding instances to the subnet group")
for i in range(4):
    subnet_group_controller.add_instance_to_subnet_group("load_balance_group", f"i-{i+1}")

load_balanced_group = subnet_group_controller.get_db_subnet_group("load_balance_group")

print("\nLoad Balancing Details:")
print(f"Number of subnets: {len(load_balanced_group.subnets)}")
print("Subnet information:")
for subnet in load_balanced_group.subnets:
    print(f"  - Subnet ID: {subnet['subnet_id']}")
    print(f"    IP Range: {subnet['ip_range']}")
    print(f"    Availability Zone: {subnet['availability_zone']}")

print("\nResource allocation:")
for i, subnet in enumerate(load_balanced_group.subnets):
    print(f"Subnet {i+1} ({subnet['subnet_id']}):")
    print(f"  Allocated instances: {', '.join(subnet['instances'])}")
    print(f"  Current load: {subnet['current_load']}")

print("\nFinal load distribution across subnets:")
print(f"Subnets in load_balance_group: {[[subnet['subnet_id'], subnet['current_load']] for subnet in load_balanced_group.subnets]}")
print("Load balancing demonstration completed")
time.sleep(4)

print("\n===================== End Of DBSubnetGroup Demonstration =====================")
print(f"Demonstration ended at: {datetime.now()}")
print(f"Total duration: {datetime.now() - start_time}")
