from sqlite3 import IntegrityError
from typing import List, Dict, Any

import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../..")))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


from Models.DBSubnetGroupModel import DBSubnetGroup
from DataAccess.DBSubnetGroupManager import DBSubnetGroupManager
from Storage.KT_Storage.DataAccess.StorageManager import StorageManager
from Storage.KT_Storage.DataAccess.VersionManager import VersionManager
from Validation.GeneralValidations import *


class DBSubnetGroupService:
    def __init__(self, db_subnet_group_manager: DBSubnetGroupManager):
        self.manager = db_subnet_group_manager
        self.bucket = "db_subnet_groups"
        self.storage_manager = StorageManager()
        self.storage_manager.create_bucket(self.bucket)
        self.version_manager = VersionManager()
        self.subnet_groups = dict()

    def create_db_subnet_group(self, **kwargs):
        # object
        if not kwargs.get("db_subnet_group_name"):
            raise ValueError("Missing required argument db_subnet_group_name")

        if not is_length_in_range(kwargs["db_subnet_group_name"], 1, 255):
            raise ValueError(
                "invalid length for subnet group db_subnet_group_name: "
                + len(kwargs["db_subnet_group_name"])
            )

        if kwargs["db_subnet_group_name"] in self.subnet_groups:
            raise ValueError(
                f"db_subnet_group_name {kwargs['db_subnet_group_name']} already exists"
            )

        if kwargs.get("description") and not is_length_in_range("description", 1, 255):
            raise ValueError(
                "invalid length for subnet group description: "
                + len(kwargs["description"])
            )

        subnet_group = DBSubnetGroup(**kwargs)
        # save in management table
        # in try except block in case the server was shut down and re-run and local collection doesn't include all subnetGroups
        try:
            self.manager.create(subnet_group)
        except IntegrityError as e:
            raise ValueError(
                f"db_subnet_group_name {kwargs['db_subnet_group_name']} already exists"
            )

        # physical object
        # version = 0 assume created for the first time
        self.storage_manager.create(
            self.bucket, subnet_group.db_subnet_group_name, subnet_group.to_bytes(), "0"
        )
        # save in local collection (hash table) for quick access
        self.subnet_groups[kwargs["db_subnet_group_name"]] = subnet_group

    def get_db_subnet_group(self, db_subnet_group_name: str) -> DBSubnetGroup:
        try:
            data = self.subnet_groups[db_subnet_group_name]
        except KeyError:
            data = self.manager.get(db_subnet_group_name)
            self.subnet_groups[db_subnet_group_name] = data
        return data

    def modify_db_subnet_group(
        self, db_subnet_group_name: str, **updates
    ) -> DBSubnetGroup:
        if not db_subnet_group_name:
            raise ValueError("Missing required argument db_subnet_group_name")

        if updates.get("description") and not is_length_in_range(
            updates["description"], 1, 255
        ):
            raise ValueError(
                "invalid length for subnet group description: "
                + len(updates["description"])
            )

        subnet_group = self.get_db_subnet_group(db_subnet_group_name)

        for key, value in updates.items():
            setattr(subnet_group, key, value)

        self.manager.modify(subnet_group)
        # version = str(int(self.version_manager.get(self.bucket, subnet_group.db_subnet_group_name).version_id)+1)
        # for now we override the basic version, when the latest version id can be retrieved, we will make a new version as old_version_id + 1
        self.storage_manager.create(
            self.bucket, db_subnet_group_name, subnet_group.to_bytes(), "0"
        )

    def delete_db_subnet_group(self, db_subnet_group_name: str) -> None:
        if not db_subnet_group_name:
            raise ValueError("Missing required argument db_subnet_group_name")

        # delete from management table
        self.manager.delete(db_subnet_group_name)
        # for now version id is 0
        # delete physical object from storage
        self.storage_manager.delete_by_name(
            bucket_name=self.bucket, version_id="0", key=db_subnet_group_name
        )
        # delete from local collection (hash table)
        if db_subnet_group_name in self.subnet_groups:
            del self.subnet_groups[db_subnet_group_name]

    def describe_db_subnet_group(self, db_subnet_group_name: str) -> Dict:
        if not db_subnet_group_name:
            raise ValueError("Missing required argument db_subnet_group_name")

        return self.manager.describe(db_subnet_group_name)

    def list_db_subnet_groups(self):
        return self.manager.list_db_subnet_groups()
