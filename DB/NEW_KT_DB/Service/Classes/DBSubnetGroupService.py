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
        self.load_balancer = LoadBalancer()

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

    
    def get_preferable_subnet_from_group(self, db_subnet_group_name):
        # get the subnets in this group
        subnet_group = self.manager.get(db_subnet_group_name)
        if not subnet_group:
            raise DBSubnetGroupExceptions.DBSubnetGroupNotFound
        
        best_subnet = self.load_balancer.select_best_subnet(subnet_group["subnets"])
        
        return best_subnet

    def assign_instance_to_subnet(self, db_subnet_group_name, subnet_id, instance_id):
        
        # Get the subnet group by name
        subnet_group = self.get_db_subnet_group(db_subnet_group_name)
        if not subnet_group:
            raise DBSubnetGroupExceptions.DBSubnetGroupNotFound

        # Find the subnet within the group
        target_subnet = None
        for subnet in subnet_group.subnets:
            if subnet.subnet_id == subnet_id:
                target_subnet = subnet
                break

        if not target_subnet:
            raise DBSubnetGroupExceptions.SubnetNotFoundInGroup(f"Subnet {subnet_id} not found in group {db_subnet_group_name}")

        # Assign the instance to the subnet
        target_subnet.assign_instance(instance_id)

        # Update the subnet group in the manager
        self.manager.modify(subnet_group)

        # Update the storage
        self.storage_manager.write_to_file(
            self.bucket + '/' + db_subnet_group_name, subnet_group.to_str()
        )

        return target_subnet   

    def unassign_instance_from_subnet(self, db_subnet_group_name, subnet_id, instance_id):
        db_subnet_group = self.get_db_subnet_group(db_subnet_group_name)
        if db_subnet_group:
            subnet = next((s for s in db_subnet_group.subnets if s.subnet_id == subnet_id), None)
            if subnet:
                if instance_id in subnet.instances:
                    subnet.remove_instance(instance_id)
                else:
                    raise ValueError(f"Instance {instance_id} is not assigned to subnet {subnet_id}")
            else:
                raise ValueError(f"Subnet {subnet_id} not found in DB subnet group {db_subnet_group_name}")
        else:
            raise ValueError(f"DB subnet group {db_subnet_group_name} not found")
        return False
    
class LoadBalancer:
    def __init__(self):
        pass

    def select_best_subnet(self, subnets):
        # Evaluate each subnet based on load (e.g., number of active instances)
        best_subnet = None
        lowest_load = float('inf')

        for subnet in subnets:
            load = subnet.get_load()
            if load < lowest_load:
                lowest_load = load
                best_subnet = subnet

        return best_subnet
