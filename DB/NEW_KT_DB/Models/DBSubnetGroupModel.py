from typing import List, Dict, Any
import ast
import json
import sys
import os


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

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
        status VARCHAR(50) DEFAULT 'pending'
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
            else:
                if args:
                    print("\033[1;31mWarning: args received in DBSubnetGroup constructor, validations can't be easily performed\033[0m")
                    try:
                        self.db_subnet_group_name = args[0]
                        self.db_subnet_group_description = args[1]
                        self.vpc_id = args[2]
                        # allow for optional parameters not to be sent when using args and not kwargs?
                        self.subnets = args[3]
                        self.db_subnet_group_arn = args[4]
                        self.status = args[5]
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
                self.subnets = ast.literal_eval(self.subnets)
            # if subnets were received as a list of strings
            try:
                if len(self.subnets) > 0 and type(self.subnets[0]) is not dict:
                    self.subnets = [ast.literal_eval(subnet) for subnet in self.subnets]
            except TypeError:
                raise ValueError("Invalid subnets format")

        except KeyError as e:
            raise DBSubnetGroupExceptions.MissingRequiredArgument(self.db_subnet_group_name)

        # Ideally:
        # self.db_subnet_group_arn should be dynamically created according to vpc-id, account-id and
        # subnet-group-name, and then dynamically added to the routing table
        
        if not self.status and self.db_subnet_group_arn:
            self.status = "available"
            
        self.pk_value = self.db_subnet_group_name

    def to_dict(self) -> Dict[str, Any]:
        return {
            "db_subnet_group_name": self.db_subnet_group_name,
            "db_subnet_group_description": self.db_subnet_group_description,
            "vpc_id": self.vpc_id,
            "subnets": self.subnets,
            "db_subnet_group_arn": self.db_subnet_group_arn,
            "status": self.status,
        }

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