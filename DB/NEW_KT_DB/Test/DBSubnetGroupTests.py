from sqlite3 import IntegrityError
import pytest
import json

import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), ".")))

from Service.Classes.DBSubnetGroupService import DBSubnetGroupService
from DataAccess.DBSubnetGroupManager import DBSubnetGroupManager
from Controller.DBSubnetGroupController import DBSubnetGroupController
from Storage.KT_Storage.DataAccess.StorageManager import StorageManager
from DataAccess.ObjectManager import ObjectManager
from Models.DBSubnetGroupModel import DBSubnetGroup
import sqlite3

object_manager = ObjectManager("../object_management_db.db")
manager = DBSubnetGroupManager(object_manager=object_manager)
service = DBSubnetGroupService(manager)
controller = DBSubnetGroupController(service)
storage_manager = StorageManager()


@pytest.fixture
def clear_table():
    # Connect to the SQLite database
    conn = conn = sqlite3.connect("../object_management_db.db")
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

    # remove existing subnet group from previous tests
    controller.create_db_subnet_group(
        db_subnet_group_name="subnet_group_1",
        subnets=[{"subnet_id": "subnet-12345678"}, {"subnet_id": "subnet-87654321"}],
        db_subnet_group_description="Test subnet group",
        vpc_id="vpc-12345678",
        db_subnet_group_arn="arn:aws:rds:us-west-2:123456789012:subgrp:subnet_group_1",
    )

    # check that file was created (no error raised on get)
    storage_manager.get("db_subnet_groups", "subnet_group_1", "0")
    # check that object was saved to management table (no error raised on get)
    controller.get_db_subnet_group("subnet_group_1")

    # check that file content is correct
    from_storage = DBSubnetGroup(
        **DBSubnetGroup.from_bytes_to_dict(
            storage_manager.get("db_subnet_groups", "subnet_group_1", "0")["content"]
        )
    )
    from_db = controller.get_db_subnet_group("subnet_group_1")
    # check values were stored correctly in management table as well as storage

    # group name
    assert from_storage.db_subnet_group_name == "subnet_group_1"
    assert from_db.db_subnet_group_name == "subnet_group_1"

    # subnets
    for subnet in from_storage.subnets:
        assert subnet in [
            {"subnet_id": "subnet-12345678"},
            {"subnet_id": "subnet-87654321"},
        ]
    for subnet in from_db.subnets:
        assert subnet in [
            {"subnet_id": "subnet-12345678"},
            {"subnet_id": "subnet-87654321"},
        ]

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
    with pytest.raises(ValueError):
        controller.create_db_subnet_group(
            db_subnet_group_name="subnet_group_1",
            subnets=[
                {"subnet_id": "subnet-87654321"},
                {"subnet_id": "subnet-12345678"},
            ],
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
    assert subnet_group_1.status == "pending"
    for subnet in subnet_group_1.subnets:
        assert subnet in [
            {"subnet_id": "subnet-12345678"},
            {"subnet_id": "subnet-87654321"},
        ]


def test_modify():
    controller.modify_db_subnet_group(
        "subnet_group_1",
        subnets=[{"subnet_id": "subnet-12345988"}, {"subnet_id": "subnet-876543881"}],
    )

    from_storage = DBSubnetGroup(
        **DBSubnetGroup.from_bytes_to_dict(
            storage_manager.get("db_subnet_groups", "subnet_group_1", "0")["content"]
        )
    )
    from_db = controller.get_db_subnet_group("subnet_group_1")
    assert from_storage.db_subnet_group_name == from_db.db_subnet_group_name
    for subnet in from_storage.subnets:
        assert subnet in from_db.subnets
    assert (
        from_storage.db_subnet_group_description == from_db.db_subnet_group_description
    )
    assert from_storage.vpc_id == from_db.vpc_id
    assert from_storage.db_subnet_group_arn == from_db.db_subnet_group_arn
    assert from_storage.status == from_db.status


def test_describe():
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
    assert subnet_group_1.status == "pending"
    for subnet in subnet_group_1.subnets:
        assert subnet in [
            {"subnet_id": "subnet-12345988"},
            {"subnet_id": "subnet-876543881"},
        ]


def test_delete():
    controller.delete_db_subnet_group("subnet_group_1")
    with pytest.raises(FileNotFoundError):
        storage_manager.get("db_subnet_groups", "subnet_group_1", "0")
    with pytest.raises(Exception):
        controller.get_db_subnet_group("subnet_group_1")


@pytest.mark.parametrize("index", range(20))
def test_insert_many(index):
    controller.create_db_subnet_group(
        db_subnet_group_name=f"subnet_group_{index}",
        subnets=[
            {"subnet_id": f"subnet-1234567{index}"},
            {"subnet_id": f"subnet-8765432{index}"},
        ],
        db_subnet_group_description=f"Test subnet group {index}",
        vpc_id=f"vpc-1234567{index}",
        db_subnet_group_arn=f"arn:aws:rds:us-west-2:123456789012:subgrp:subnet_group_{index}",
    )

    # check that file was created (no error raised on get)
    storage_manager.get("db_subnet_groups", f"subnet_group_{index}", "0")
    # check that object was saved to management table (no error raised on get)
    controller.get_db_subnet_group(f"subnet_group_{index}")

    # check that file content is correct
    from_storage = DBSubnetGroup(
        **DBSubnetGroup.from_bytes_to_dict(
            storage_manager.get("db_subnet_groups", f"subnet_group_{index}", "0")[
                "content"
            ]
        )
    )
    from_db = controller.get_db_subnet_group(f"subnet_group_{index}")
    # check values were stored correctly in management table as well as storage

    # group name
    assert from_storage.db_subnet_group_name == f"subnet_group_{index}"
    assert from_db.db_subnet_group_name == f"subnet_group_{index}"

    # subnets
    for subnet in from_storage.subnets:
        assert subnet in [
            {"subnet_id": f"subnet-1234567{index}"},
            {"subnet_id": f"subnet-8765432{index}"},
        ]
    for subnet in from_db.subnets:
        assert subnet in [
            {"subnet_id": f"subnet-1234567{index}"},
            {"subnet_id": f"subnet-8765432{index}"},
        ]

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
        storage_manager.get("db_subnet_groups", db_subnet_group_name, "0")
    with pytest.raises(Exception):
        controller.get_db_subnet_group(db_subnet_group_name)


import pytest
import random
import string


def generate_random_string(length):
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=length))


@pytest.mark.parametrize("num_subnets", [1, 5, 20])
def test_create_db_subnet_group_with_varying_subnets(num_subnets):
    vpc_id = f"vpc-{generate_random_string(8)}"
    subnets = [
        {"subnet_id": f"subnet-{generate_random_string(8)}"} for _ in range(num_subnets)
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
    for subnet in subnets:
        assert subnet in from_db.subnets

    # Clean up
    controller.delete_db_subnet_group(db_subnet_group_name)


@pytest.mark.parametrize("num_groups", [5, 10, 20])
def test_create_multiple_db_subnet_groups(num_groups):
    vpc_id = f"vpc-{generate_random_string(8)}"
    groups = []

    for _ in range(num_groups):
        db_subnet_group_name = f"test-group-{generate_random_string(8)}"
        description = f"Test subnet group {_}"
        subnets = [
            {"subnet_id": f"subnet-{generate_random_string(8)}"} for _ in range(2)
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
        {"subnet_id": f"subnet-{generate_random_string(8)}"} for _ in range(2)
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
    new_subnet = {"subnet_id": f"subnet-{generate_random_string(8)}"}
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
        assert subnet in updated_subnets

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
            {"subnet_id": f"subnet-{generate_random_string(8)}"} for _ in range(2)
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
            {"subnet_id": f"subnet-{generate_random_string(8)}"} for _ in range(2)
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
