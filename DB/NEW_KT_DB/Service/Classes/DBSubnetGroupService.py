from sqlite3 import IntegrityError
from typing import List, Dict, Any
import Exceptions.DBSubnetGroupExceptions as DBSubnetGroupExceptions
import Validation.DBSubnetGroupValidations as DBSubnetGroupValidations
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../..")))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


from Models.DBSubnetGroupModel import DBSubnetGroup
from DataAccess.DBSubnetGroupManager import DBSubnetGroupManager
from Storage.NEW_KT_Storage.DataAccess.StorageManager import StorageManager


class DBSubnetGroupService:
    def __init__(self, db_subnet_group_manager: DBSubnetGroupManager, storage_manager: StorageManager):
        self.manager = db_subnet_group_manager
        self.bucket = "db_subnet_groups"
        self.storage_manager = storage_manager
        self.storage_manager.create_directory(self.bucket)
        self.subnet_groups = dict()

    def create_db_subnet_group(self, **kwargs):
        # validate the arguments
        if not kwargs.get("db_subnet_group_name"):
            raise DBSubnetGroupExceptions.MissingRequiredArgument("db_subnet_group_name")

        if kwargs["db_subnet_group_name"] in self.subnet_groups.keys():
            raise DBSubnetGroupExceptions.DBSubnetGroupAlreadyExists(
                kwargs["db_subnet_group_name"] 
            )
        
        # validate the arguments
        DBSubnetGroupValidations.validate_subnet_group_name(kwargs["db_subnet_group_name"])
        DBSubnetGroupValidations.validate_subnet_group_description(kwargs["db_subnet_group_description"])
        
        try:
            c = kwargs["vpc_id"]
        except KeyError:
            raise DBSubnetGroupExceptions.MissingRequiredArgument("vpc_id")
        
        # create an object from the arguments received
        subnet_group = DBSubnetGroup(**kwargs)
        # save in management table
        # in try except block in case the server was shut down and re-run and local collection doesn't include all subnetGroups
        try:
            self.manager.create(subnet_group)
        except IntegrityError as e:
            raise DBSubnetGroupExceptions.DBSubnetGroupAlreadyExists(
                f"{kwargs['db_subnet_group_name']}"
            )

        # physical object
        self.storage_manager.create_file(
            self.bucket + "/" + subnet_group.db_subnet_group_name, subnet_group.to_str()
        )
        # save in local collection (hash table) for quick access
        self.subnet_groups[kwargs["db_subnet_group_name"]] = subnet_group

    def get_db_subnet_group(self, db_subnet_group_name: str) -> DBSubnetGroup:
        # try to get the object from the local collection (hash table)
        try:
            data = self.subnet_groups[db_subnet_group_name]
        # if it's not in the collection, get it from the manager and save it in the collection
        except KeyError:
            data = self.manager.get(db_subnet_group_name)
            self.subnet_groups[db_subnet_group_name] = data
        return data

    def modify_db_subnet_group(self, db_subnet_group_name: str, **updates) -> DBSubnetGroup:
        if not db_subnet_group_name:
            raise ValueError("Missing required argument db_subnet_group_name")

        # validate the arguments
        DBSubnetGroupValidations.validate_subnet_group_name(db_subnet_group_name)
        try:
            DBSubnetGroupValidations.validate_subnet_group_description(updates.get("db_subnet_group_description"))
        except KeyError:
            pass

        # get the object 
        subnet_group = self.get_db_subnet_group(db_subnet_group_name)

        # update the object based ont the arguments sent
        for key, value in updates.items():
            setattr(subnet_group, key, value)

        # send to DBSubnetGroupManager to modify in DB
        self.manager.modify(subnet_group)
        # version = str(int(self.version_manager.get(self.bucket, subnet_group.db_subnet_group_name).version_id)+1)
        # for now we override the basic version, when the latest version id can be retrieved, we will make a new version as old_version_id + 1
        self.storage_manager.write_to_file(
            self.bucket + '/' + db_subnet_group_name, subnet_group.to_str()
        )
        
        return subnet_group

    def delete_db_subnet_group(self, db_subnet_group_name: str) -> None:
        if not db_subnet_group_name:
            raise DBSubnetGroupExceptions.MissingRequiredArgument("db_subnet_group_name")
        # delete from management table
        self.manager.delete(db_subnet_group_name)
        # for now version id is 0
        # delete physical object from storage
        self.storage_manager.delete_file(
            self.bucket + '/' + db_subnet_group_name
        )
        # delete from local collection (hash table)
        if db_subnet_group_name in self.subnet_groups:
            del self.subnet_groups[db_subnet_group_name]

    def describe_db_subnet_group(self, db_subnet_group_name: str) -> Dict:
        return self.manager.describe(db_subnet_group_name)

    def list_db_subnet_groups(self):
        return self.manager.list_db_subnet_groups()
