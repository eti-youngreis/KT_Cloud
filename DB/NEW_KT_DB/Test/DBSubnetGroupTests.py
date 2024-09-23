import pytest
import random
import string

import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), ".")))

from Service.Classes.DBSubnetGroupService import DBSubnetGroupService
from DataAccess.DBSubnetGroupManager import DBSubnetGroupManager
from Controller.DBSubnetGroupController import DBSubnetGroupController
from Storage.NEW_KT_Storage.DataAccess.StorageManager import StorageManager
from DataAccess.ObjectManager import ObjectManager
from Models.DBSubnetGroupModel import DBSubnetGroup
from Models.Subnet import Subnet
import Exceptions.DBSubnetGroupExceptions as DBSubnetGroupExceptions
import sqlite3

object_manager = ObjectManager("../DBs/mainDB.db")

manager = DBSubnetGroupManager(object_manager=object_manager)
storage_manager = StorageManager("DB/s3")

service = DBSubnetGroupService(manager, storage_manager=storage_manager)
controller = DBSubnetGroupController(service)


@pytest.fixture
def clear_table():
    # Connect to the SQLite database
    conn = conn = sqlite3.connect("../DBs/mainDB.db")
    cursor = conn.cursor()

    # Clear the table if it exists
    table_name = "mng_DBSubnetGroups"
    cursor.execute(f"DELETE FROM {table_name};")

    # Commit changes and close the connection
    conn.commit()
    conn.close()

    # Yield to allow tests to run
    yield


def test_create(clear_table):

    subnets = [
        Subnet(subnet_id="subnet-12345678", ip_range="10.0.0.0/24"),
        Subnet(subnet_id="subnet-87654321", ip_range="10.0.1.0/24"),
    ]
    controller.create_db_subnet_group(
        db_subnet_group_name="subnet_group_1",
        subnets=subnets,
        db_subnet_group_description="Test subnet group",
        vpc_id="vpc-12345678",
        db_subnet_group_arn="arn:aws:rds:us-west-2:123456789012:subgrp:subnet_group_1",
    )

    # check that file was created (no error raised on get)
    storage_manager.is_file_exist("db_subnet_groups/subnet_group_1")
    # check that object was saved to management table (no error raised on get)
    controller.get_db_subnet_group("subnet_group_1")

    # check that file content is correct
    # storage manager doesn't have a get or read function
    # from_storage = DBSubnetGroup(
    #     **DBSubnetGroup.from_bytes_to_dict(
    #         storage_manager.get("db_subnet_groups", "subnet_group_1", "0")["content"]
    #     )
    # )

    file = open("DB/s3/db_subnet_groups/subnet_group_1", "r")
    str_data = file.read()
    file.close()
    from_storage = DBSubnetGroup.from_str(str_data)
    from_db = controller.get_db_subnet_group("subnet_group_1")
    # check values were stored correctly in management table as well as storage

    # group name
    assert from_storage.db_subnet_group_name == "subnet_group_1"
    assert from_db.db_subnet_group_name == "subnet_group_1"

    # subnets
    for subnet in from_storage.subnets:
        assert subnet.subnet_id in [subnet.subnet_id for subnet in from_storage.subnets]
    for subnet in from_db.subnets:
        assert subnet in subnets

    # description
    assert from_storage.db_subnet_group_description == "Test subnet group"
    assert from_db.db_subnet_group_description == "Test subnet group"

    # vpc_id
    assert from_storage.vpc_id == "vpc-12345678"
    assert from_db.vpc_id == "vpc-12345678"

    # arn
    assert (
        from_storage.db_subnet_group_arn
        == "arn:aws:rds:us-west-2:123456789012:subgrp:subnet_group_1"
    )
    assert (
        from_db.db_subnet_group_arn
        == "arn:aws:rds:us-west-2:123456789012:subgrp:subnet_group_1"
    )


def test_unique_constraint():
    subnets = [
        Subnet(subnet_id="subnet-87654321", ip_range="10.0.1.0/24"),
        Subnet(subnet_id="subnet-12345678", ip_range="10.0.0.0/24"),
    ]
    with pytest.raises(DBSubnetGroupExceptions.DBSubnetGroupAlreadyExists):
        controller.create_db_subnet_group(
            db_subnet_group_name="subnet_group_1",
            subnets=subnets,
            db_subnet_group_description="Another subnet group with same name",
            vpc_id="vpc-87654321",
            db_subnet_group_arn="arn:aws:rds:us-west-2:123456789012:subgrp:subnet_group_1",
        )


def test_get():
    subnet_group_1 = controller.get_db_subnet_group("subnet_group_1")
    assert subnet_group_1 != None
    assert subnet_group_1.db_subnet_group_name == "subnet_group_1"
    assert subnet_group_1.db_subnet_group_description == "Test subnet group"
    assert subnet_group_1.vpc_id == "vpc-12345678"
    assert (
        subnet_group_1.db_subnet_group_arn
        == "arn:aws:rds:us-west-2:123456789012:subgrp:subnet_group_1"
    )
    assert subnet_group_1.status == "available"
    for subnet in subnet_group_1.subnets:
        assert subnet.subnet_id in ["subnet-12345678", "subnet-87654321"]


def test_modify():
    subnets = [
        Subnet(subnet_id="subnet-12345988", ip_range="10.0.2.0/24"),
        Subnet(subnet_id="subnet-876543881", ip_range="10.0.3.0/24"),
    ]
    controller.modify_db_subnet_group("subnet_group_1", subnets=subnets)
    file = open("DB/s3/db_subnet_groups/subnet_group_1", "r")
    from_storage = DBSubnetGroup.from_str(file.read())
    file.close()

    from_db = controller.get_db_subnet_group("subnet_group_1")
    assert from_storage.db_subnet_group_name == from_db.db_subnet_group_name
    for subnet in from_storage.subnets:
        assert subnet.subnet_id in ["subnet-12345988", "subnet-876543881"]

    for subnet in from_db.subnets:
        assert subnet.subnet_id in ["subnet-12345988", "subnet-876543881"]
    assert (
        from_storage.db_subnet_group_description == from_db.db_subnet_group_description
    )
    assert from_storage.vpc_id == from_db.vpc_id
    assert from_storage.db_subnet_group_arn == from_db.db_subnet_group_arn
    assert from_storage.status == from_db.status


def test_describe():
    subnets = [
        Subnet(subnet_id="subnet-12345988", ip_range="10.0.2.0/24"),
        Subnet(subnet_id="subnet-876543881", ip_range="10.0.3.0/24"),
    ]
    subnet_group_1 = controller.describe_db_subnet_group("subnet_group_1")
    assert type(subnet_group_1) == dict
    assert type(subnet_group_1["subnets"]) == list
    assert type(subnet_group_1["subnets"][0]) == dict
    subnet_group_1 = DBSubnetGroup(**subnet_group_1)
    assert subnet_group_1 != None
    assert subnet_group_1.db_subnet_group_name == "subnet_group_1"
    assert subnet_group_1.db_subnet_group_description == "Test subnet group"
    assert subnet_group_1.vpc_id == "vpc-12345678"
    assert (
        subnet_group_1.db_subnet_group_arn
        == "arn:aws:rds:us-west-2:123456789012:subgrp:subnet_group_1"
    )
    assert subnet_group_1.status == "available"
    for subnet in subnet_group_1.subnets:
        assert subnet.subnet_id in ["subnet-12345988", "subnet-876543881"]


def test_delete():
    controller.delete_db_subnet_group("subnet_group_1")
    with pytest.raises(FileNotFoundError):
        file = open("DB/s3/db_subnet_groups/subnet_group_1", "r")
        from_storage = DBSubnetGroup.from_str(file.read())
        file.close()
    with pytest.raises(DBSubnetGroupExceptions.DBSubnetGroupNotFound):
        controller.get_db_subnet_group("subnet_group_1")


@pytest.mark.parametrize("index", range(20))
def test_insert_many(index):
    subnets = [
        Subnet(subnet_id=f"subnet-1234567{index}", ip_range=f"10.0.{index}.0/24"),
        Subnet(subnet_id=f"subnet-8765432{index}", ip_range=f"10.0.{index+1}.0/24"),
    ]
    controller.create_db_subnet_group(
        db_subnet_group_name=f"subnet_group_{index}",
        subnets=subnets,
        db_subnet_group_description=f"Test subnet group {index}",
        vpc_id=f"vpc-1234567{index}",
        db_subnet_group_arn=f"arn:aws:rds:us-west-2:123456789012:subgrp:subnet_group_{index}",
    )

    # check that file was created (no error raised on get)
    file = open(f"DB/s3/db_subnet_groups/subnet_group_{index}", "r")
    from_storage = DBSubnetGroup.from_str(file.read())
    file.close()
    # check that object was saved to management table (no error raised on get)
    controller.get_db_subnet_group(f"subnet_group_{index}")

    # check that file content is correct
    # from_storage = DBSubnetGroup(
    #     **DBSubnetGroup.from_bytes_to_dict(
    #         storage_manager.get("db_subnet_groups", f"subnet_group_{index}", "0")[
    #             "content"
    #         ]
    #     )
    # )
    file = open(f"DB/s3/db_subnet_groups/subnet_group_{index}", "r")
    from_storage = DBSubnetGroup.from_str(file.read())
    file.close()
    from_db = controller.get_db_subnet_group(f"subnet_group_{index}")
    # check values were stored correctly in management table as well as storage

    # group name
    assert from_storage.db_subnet_group_name == f"subnet_group_{index}"
    assert from_db.db_subnet_group_name == f"subnet_group_{index}"

    # subnets
    for subnet in from_storage.subnets:
        assert subnet.subnet_id in [s.subnet_id for s in subnets]
    for subnet in from_db.subnets:
        assert subnet.subnet_id in [s.subnet_id for s in subnets]

    # description
    assert from_storage.db_subnet_group_description == f"Test subnet group {index}"
    assert from_db.db_subnet_group_description == f"Test subnet group {index}"

    # vpc_id
    assert from_storage.vpc_id == f"vpc-1234567{index}"
    assert from_db.vpc_id == f"vpc-1234567{index}"

    # arn
    assert (
        from_storage.db_subnet_group_arn
        == f"arn:aws:rds:us-west-2:123456789012:subgrp:subnet_group_{index}"
    )
    assert (
        from_db.db_subnet_group_arn
        == f"arn:aws:rds:us-west-2:123456789012:subgrp:subnet_group_{index}"
    )


@pytest.mark.parametrize("index", range(20))
def test_delete_many_from_prev_test(index):
    db_subnet_group_name = f"subnet_group_{index}"
    controller.delete_db_subnet_group(db_subnet_group_name)
    with pytest.raises(FileNotFoundError):
        file = open(f"DB/s3/db_subnet_groups/subnet_group_{index}", "r")
        from_storage = DBSubnetGroup.from_str(file.read())
        file.close()
    with pytest.raises(DBSubnetGroupExceptions.DBSubnetGroupNotFound):
        controller.get_db_subnet_group(db_subnet_group_name)


# read and understand all
def generate_random_string(length):
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=length))


@pytest.mark.parametrize("num_subnets", [1, 5, 20])
def test_create_db_subnet_group_with_varying_subnets(num_subnets):
    vpc_id = f"vpc-{generate_random_string(8)}"
    subnets = [
        Subnet(
            subnet_id=f"subnet-{generate_random_string(8)}", ip_range=f"10.0.{i}.0/24"
        )
        for i in range(num_subnets)
    ]
    db_subnet_group_name = f"test-group-{generate_random_string(8)}"
    description = f"Test subnet group with {num_subnets} subnets"

    controller.create_db_subnet_group(
        db_subnet_group_name=db_subnet_group_name,
        db_subnet_group_description=description,
        subnets=subnets,
        vpc_id=vpc_id,
    )

    # Verify the created group
    from_db = controller.get_db_subnet_group(db_subnet_group_name)
    print(from_db.subnets)
    assert from_db.db_subnet_group_name == db_subnet_group_name
    assert from_db.db_subnet_group_description == description
    assert from_db.vpc_id == vpc_id
    assert len(from_db.subnets) == num_subnets
    for subnet in from_db.subnets:
        assert subnet.subnet_id in [s.subnet_id for s in subnets]

    # Clean up
    controller.delete_db_subnet_group(db_subnet_group_name)


@pytest.mark.parametrize("num_groups", [5, 10, 20])
def test_create_multiple_db_subnet_groups(num_groups):
    vpc_id = f"vpc-{generate_random_string(8)}"
    groups = []

    for i in range(num_groups):
        db_subnet_group_name = f"test-group-{generate_random_string(8)}"
        description = f"Test subnet group {i}"
        subnets = [
            Subnet(
                subnet_id=f"subnet-{generate_random_string(8)}",
                ip_range=f"10.0.{i}.0/24",
            )
            for i in range(2)
        ]

        controller.create_db_subnet_group(
            db_subnet_group_name=db_subnet_group_name,
            db_subnet_group_description=description,
            subnets=subnets,
            vpc_id=vpc_id,
        )
        groups.append(db_subnet_group_name)

    # Verify all groups were created
    for group_name in groups:
        from_db = controller.get_db_subnet_group(group_name)
        assert from_db.db_subnet_group_name == group_name
        assert from_db.vpc_id == vpc_id

    # Clean up
    for group_name in groups:
        controller.delete_db_subnet_group(group_name)


def test_update_db_subnet_group():
    vpc_id = f"vpc-{generate_random_string(8)}"
    initial_subnets = [
        Subnet(
            subnet_id=f"subnet-{generate_random_string(8)}", ip_range=f"10.0.{_}.0/24"
        )
        for _ in range(2)
    ]
    db_subnet_group_name = f"test-group-{generate_random_string(8)}"
    initial_description = "Initial description"

    controller.create_db_subnet_group(
        db_subnet_group_name=db_subnet_group_name,
        db_subnet_group_description=initial_description,
        subnets=initial_subnets,
        vpc_id=vpc_id,
    )

    # Update the group
    new_description = "Updated description"
    new_subnet = Subnet(
        subnet_id=f"subnet-{generate_random_string(8)}", ip_range=f"10.0.2.0/24"
    )
    updated_subnets = initial_subnets + [new_subnet]

    controller.modify_db_subnet_group(
        db_subnet_group_name,
        db_subnet_group_description=new_description,
        subnets=updated_subnets,
    )

    # Verify the update
    from_db = controller.get_db_subnet_group(db_subnet_group_name)
    assert from_db.db_subnet_group_name == db_subnet_group_name
    assert from_db.db_subnet_group_description == new_description
    assert from_db.vpc_id == vpc_id
    assert len(from_db.subnets) == len(updated_subnets)
    for subnet in from_db.subnets:
        assert subnet.subnet_id in [s.subnet_id for s in updated_subnets]

    # Clean up
    controller.delete_db_subnet_group(db_subnet_group_name)


def test_delete_nonexistent_db_subnet_group():
    non_existent_group_name = f"non-existent-group-{generate_random_string(8)}"

    with pytest.raises(Exception):
        controller.delete_db_subnet_group(non_existent_group_name)


@pytest.mark.parametrize("num_operations", [50, 100, 200])
def test_concurrent_operations(num_operations):
    vpc_id = f"vpc-{generate_random_string(8)}"
    group_names = [
        f"test-group-{generate_random_string(8)}" for _ in range(num_operations)
    ]

    # Create groups
    for group_name in group_names:
        subnets = [
            Subnet(
                subnet_id=f"subnet-{generate_random_string(8)}",
                ip_range=f"10.0.{_}.0/24",
            )
            for _ in range(2)
        ]
        controller.create_db_subnet_group(
            db_subnet_group_name=group_name,
            db_subnet_group_description=f"Test group {group_name}",
            subnets=subnets,
            vpc_id=vpc_id,
        )

    # Perform random operations
    for _ in range(num_operations):
        operation = random.choice(["get", "update", "delete"])
        group_name = random.choice(group_names)

        if operation == "get":
            try:
                controller.get_db_subnet_group(group_name)
            except Exception:
                pass
        elif operation == "update":
            try:
                new_description = f"Updated description for {group_name}"
                new_subnet = {"subnet_id": f"subnet-{generate_random_string(8)}"}
                controller.modify_db_subnet_group(
                    db_subnet_group_name=group_name,
                    db_subnet_group_description=new_description,
                    subnet_ids=[new_subnet["subnet_id"]],
                )
            except Exception:
                pass
        elif operation == "delete":
            try:
                controller.delete_db_subnet_group(group_name)
                group_names.remove(group_name)
            except Exception:
                pass

    # Clean up any remaining groups
    for group_name in group_names:
        controller.delete_db_subnet_group(group_name)

def test_db_subnet_group_listing():
    vpc_id = f"vpc-{generate_random_string(8)}"
    num_groups = 5
    group_names = []

    # Create groups
    for i in range(num_groups):
        group_name = f"test-group-{generate_random_string(8)}"
        group_names.append(group_name)
        subnets = [
            Subnet(
                subnet_id=f"subnet-{generate_random_string(8)}",
                ip_range=f"10.0.{_}.0/24",
            )
            for _ in range(2)
        ]
        controller.create_db_subnet_group(
            db_subnet_group_name=group_name,
            db_subnet_group_description=f"Test group {i}",
            subnet=subnets,
            vpc_id=vpc_id,
        )

    # List all groups
    all_groups = controller.list_db_subnet_groups()

    # Verify all created groups are in the list
    for group_name in group_names:
        assert any(group.db_subnet_group_name == group_name for group in all_groups)

    # Clean up
    for group_name in group_names:
        controller.delete_db_subnet_group(group_name)


def test_load_balancing():
    group = DBSubnetGroup(
        db_subnet_group_name="test_group",
        db_subnet_group_description="Test group",
        vpc_id="vpc-12345678"
    )
    subnet1 = Subnet(subnet_id="subnet-1", ip_range="10.0.1.0/24", availability_zone="us-west-2a")
    subnet2 = Subnet(subnet_id="subnet-2", ip_range="10.0.2.0/24", availability_zone="us-west-2b")
    group.add_subnet(subnet1)
    group.add_subnet(subnet2)

    instance_ids = ["i-1", "i-2", "i-3", "i-4"]
    
    for i in instance_ids:
       group.add_instance(i)
        

    assert subnet1.get_load() == 2
    assert subnet2.get_load() == 2

def test_multi_az_distribution():
    group = DBSubnetGroup(
        db_subnet_group_name="multi_az_group",
        db_subnet_group_description="Multi-AZ group",
        vpc_id="vpc-87654321"
    )
    subnet1 = Subnet(subnet_id="subnet-1", ip_range="10.0.1.0/24", availability_zone="us-west-2a")
    subnet2 = Subnet(subnet_id="subnet-2", ip_range="10.0.2.0/24", availability_zone="us-west-2b")
    group.add_subnet(subnet1)
    group.add_subnet(subnet2)

    assert group.spans_multiple_azs() == True