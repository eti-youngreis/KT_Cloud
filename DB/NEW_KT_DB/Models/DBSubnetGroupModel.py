from typing import List, Dict, Any
import ast
import json
import sys
import os


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from .Subnet import Subnet
import Exceptions.DBSubnetGroupExceptions as DBSubnetGroupExceptions


class DBSubnetGroup:

    pk_column = "db_subnet_group_name"
    object_name = "DBSubnetGroup"
    table_structure = f"""
        db_subnet_group_name primary key not null,
        db_subnet_group_description TEXT NOT NULL,
        vpc_id VARCHAR(255) NOT NULL,
        subnets JSONB DEFAULT '{[]}',
        db_subnet_group_arn VARCHAR(255),
        status VARCHAR(50) DEFAULT 'pending',
        supported_network_types JSONB DEFAULT '{[]}'
    """

    def __init__(self, *args, **kwargs):
        try:
            # prefer kwargs
            if kwargs:
                self.db_subnet_group_name = kwargs["db_subnet_group_name"]
                self.db_subnet_group_description = kwargs["db_subnet_group_description"]
                self.vpc_id = kwargs["vpc_id"]
                self.subnets = kwargs.get("subnets", None)
                self.db_subnet_group_arn = kwargs.get("db_subnet_group_arn", None)
                self.supported_network_types = kwargs.get("supported_network_types", [])

            else:
                if args:
                    try:
                        self.db_subnet_group_name = args[0]
                        self.db_subnet_group_description = args[1]
                        self.vpc_id = args[2]
                        self.subnets = args[3]
                        self.db_subnet_group_arn = args[4]
                        self.status = args[5]
                        self.supported_network_types = args[6]
                    except IndexError:
                        raise DBSubnetGroupExceptions.MissingRequiredArgument()
                else:
                    raise DBSubnetGroupExceptions.MissingRequiredArgument()
            if not hasattr(self, "status"):
                self.status = None
            # if subnets weren't provided
            if not self.subnets:
                self.subnets = []
            # if subnets were received as a string (from DB query) convert them to a list of dictionaries
            if type(self.subnets) is not list:
                self.subnets = ast.literal_eval(self.subnets.replace('null', 'None'))
            # if subnets were received as a list of strings
            try:
                if len(self.subnets) > 0 and type(list(self.subnets)[0]) is not Subnet:
                    self.subnets = [Subnet(**subnet) for subnet in self.subnets]
            except TypeError:
                raise ValueError("Invalid subnets format")

        except KeyError as e:
            raise DBSubnetGroupExceptions.MissingRequiredArgument(
                self.db_subnet_group_name
            )

        # Ideally:
        # self.db_subnet_group_arn should be dynamically created according to vpc-id, account-id and
        # subnet-group-name, and then dynamically added to the routing table

        if not self.status and self.db_subnet_group_arn:
            self.status = "available"

        self.pk_value = self.db_subnet_group_name

    def add_instance(self, instance_id: str):
        available_subnets = [subnet for subnet in self.subnets if subnet.subnet_status == "available"]
        if not available_subnets:
            raise ValueError("No available subnets in the group")
        
        else:
            subnet = min(available_subnets, key = lambda s: s.get_load())
            subnet.assign_instance(instance_id)
            return subnet
        
    def add_subnet(self, subnet: Subnet):
        self.subnets.append(subnet)

    def remove_subnet(self, subnet_id: str):
        self.subnets = [subnet for subnet in self.subnets if subnet.subnet_id != subnet_id]
    
    def spans_multiple_azs(self) -> bool:
        return len(set(subnet.availability_zone for subnet in self.subnets)) > 1

    def validate_vpc(self):
        return all(subnet.vpc_id == self.vpc_id for subnet in self.subnets)
            
    def to_dict(self):
        return {
            "db_subnet_group_name": self.db_subnet_group_name,
            "db_subnet_group_description": self.db_subnet_group_description,
            "vpc_id": self.vpc_id,
            "subnets": [subnet.to_dict() if type(subnet) != dict else subnet for subnet in self.subnets],
            "db_subnet_group_arn": self.db_subnet_group_arn,
            "status": self.status,
            "supported_network_types": self.supported_network_types
        }

    @classmethod
    def from_dict(cls, data):
        data['subnets'] = [Subnet.from_dict(subnet) for subnet in data['subnets']]
        return cls(**data)
    
    def to_bytes(self):
        bytes = json.dumps(self.to_dict())
        bytes = bytes.encode("utf-8")
        return bytes

    def from_bytes_to_dict(bytes):
        return json.loads(bytes.decode("utf-8"))

    def to_sql_insert(self):
        """converts the object into a string that can be place in a SQL insert statement"""
        # Convert the model instance to a dictionary
        data_dict = self.to_dict()
        values = (
            "("
            + ", ".join(
                (
                    f"'{json.dumps(v)}'"
                    if isinstance(v, dict) or isinstance(v, list)
                    else f"'{str(v)}'"
                )
                for v in data_dict.values()
            )
            + ")"
        )
        return values

    def to_sql_update(self):
        """convert object into a string that can be place in a SQL update statement"""
        data_dict = self.to_dict()
        del data_dict["db_subnet_group_name"]
        updates = ", ".join(
            [
                f"{k} = '{json.dumps(v) if isinstance(v, dict) or isinstance(v, list) else str(v)}'"
                for k, v in data_dict.items()
            ]
        )
        return updates

    def to_str(self):
        str_data = json.dumps(self.to_dict())
        return str_data

    def from_str(str_data):
        """convert a string into a DBSubnetGroup object"""
        data_dict = json.loads(str_data)
        return DBSubnetGroup(**data_dict)
