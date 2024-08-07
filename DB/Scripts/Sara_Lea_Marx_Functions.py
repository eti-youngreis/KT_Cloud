import json
from typing import Optional, Dict, List
import sqlite3
import Cluster
import validations
import sql_commands

class Sara_Lea_Marx_Functions:
    
    def __init__(self, db_file: str):
        self.db_file = db_file
        self.objects = {"Clusters":{},"Instances":{}}
        self.create_object_management_table()

    def open_connection(self) -> sqlite3.Connection:
        """Initialize the SQLite connection."""
        return sqlite3.connect(self.db_file)

    def create_object_management_table(self) -> None:
        """Create the object_management table if it does not exist."""
        with self.open_connection() as conn:
            c = conn.cursor()
            c.execute('''
            CREATE TABLE IF NOT EXISTS object_management (
                type_object TEXT,
                object_id TEXT,
                metadata TEXT,
                parent_id TEXT
            )
            ''')
            conn.commit()
    
    def validate_cluster_parameters(self,required_params, **kwargs):
        return validations.check_required_params(required_params,**kwargs)

    def is_valid_dbClusterIdentifier(self, dbClusterIdentifier: str) -> bool:
        """Check if the bdClusterIdentifier is valid based on the pattern and length."""
        pattern = r'^[a-zA-Z][a-zA-Z0-9]*(?:-[a-zA-Z0-9]+)*[a-zA-Z0-9]$'
        return validations.is_valid_length(dbClusterIdentifier, 1, 63) and validations.is_valid_pattern(dbClusterIdentifier, pattern)
    
    def is_valid_engineName(self, engine_name: str) -> bool:
        """Check if the engineName is valid."""
        valid_engine_names = {
          "neptune", "postgres", "mysql", "aurora-postgresql", "aurora-mysql"
        }
        return validations.string_in_dict(engine_name, valid_engine_names)
    
    def check_parameters_constarins(self, conn, **kwargs):
        """checks all the cluster parameters constrains"""

        if "db_cluster_parameter_group_name" in kwargs.keys():
            if not validations.exist_key_value_in_json_column(conn, "object_management", "object_id","db_cluster_parameter_group_name", kwargs["db_cluster_parameter_group_name"] ):
                raise ValueError(f"Parameter group name does not exist")
        
        if "db_subnet_group_name" in kwargs.keys():
            if not validations.exist_key_value_in_json_column(conn, "object_management", "object_id","db_subnet_group_name", kwargs["db_subnet_group_name"] ):
                raise ValueError(f"Subnet group name does not exist")
            
        if "port" in kwargs.keys():
            if kwargs["port"]<1150 or kwargs["port"]>65535:
                raise ValueError("Invalide port number")
            
        elif "engine" in kwargs.keys():
            if kwargs["engine"]=="MySQL" or kwargs["engine"]=="Aurora MySQL": 
                kwargs["port"]=3306
            elif kwargs["engine"]=="PostgreSQL" or kwargs["engine"]=="Aurora PostgreSQL":
                kwargs["port"]=5432
        if "storage_type" in kwargs.keys():
            if kwargs["storage_type"]!= "aurora-iopt1" and kwargs["storage_type"]!= "aurora":
                raise ValueError("Invalide storage_type value")
        else:
            kwargs["storage_type"] = "aurora"
        

    #add adding of db when db name is supplied
    def CreateDBCluster(self, **kwargs) -> str:
        """Create a new cluster and insert it into the object_management table."""
        required_params=["db_cluster_identifier","engine"]
        if not self.validate_cluster_parameters(required_params, **kwargs):
            raise ValueError(f"Missing required fields")
        if not self.is_valid_dbClusterIdentifier(kwargs["db_cluster_identifier"]):
            raise ValueError(f"Invalid dbClusterIdentifier:")
        if not self.is_valid_engineName(kwargs["engine"]):
            raise ValueError(f"Invalid engineName: ")
        with self.open_connection() as conn:
            if validations.exist_value_in_column(conn, "object_management", "object_id", kwargs["db_cluster_identifier"] ):
               raise ValueError("Cluster identifier already exist")
            self.check_parameters_constarins(conn, **kwargs)
            cluster = Cluster.Cluster(**kwargs)
            cluster.save_changes_in_management_db(conn)
            self.objects["Clusters"][kwargs["db_cluster_identifier"]] =  cluster
            return {"cluster": cluster.get_cluster_data_in_dict()}
    
    def ModifyDBCluster(self, **kwargs) -> str:
        """Modify a db cluster"""
        required_params=["db_cluster_identifier"]
        if not self.validate_cluster_parameters(required_params, **kwargs):
            raise ValueError(f"Cluster identifier is required")
        with self.open_connection() as conn:
            if  not validations.exist_value_in_column(conn, "object_management", "object_id", kwargs["db_cluster_identifier"] ):
                raise ValueError("Cluster identifier does not exist")
            self.check_parameters_constarins(conn, **kwargs)
            for key, inner_dict in self.objects.items():
                if kwargs["db_cluster_identifier"] in inner_dict:
                    cluster_obj_to_modify = inner_dict[kwargs["db_cluster_identifier"]]
                    cluster_obj_to_modify.modify_cluster(conn, **kwargs)
                    cluster_obj_to_modify.save_changes_in_management_db(conn, True)
                    return {"modified_cluster": cluster_obj_to_modify.get_cluster_data_in_dict()}

    # def StopDBCluster(self, db_cluster_identifier):
    #     """Stop a db cluster"""
    #     with self.open_connection() as conn:
    #         if not validations.exist_value_in_column(conn, "object_management", "object_id", db_cluster_identifier ):
    #             raise ValueError("Cluster identifier does not exist")

    #         for key, inner_dict in self.objects.items():
    #             if db_cluster_identifier in inner_dict:
    #                 cluster_obj_to_stop = inner_dict[db_cluster_identifier]
    #         cluster_obj_to_stop.stop_cluster(conn)
    #         cluster_obj_to_stop.status = 'stopped'
    #         del self.objects["Clusters"][db_cluster_identifier]
    #         cluster_obj_to_stop.save_changes_in_management_db(conn, True)
    #         return cluster_obj_to_stop.get_cluster_data_in_dict()
    
    # def StartDBCluster(self, db_cluster_identifier):
    #     """Start a stopped db cluster"""
    #     with self.open_connection() as conn:
    #         if not validations.exist_value_in_column(conn, "object_management", "object_id", db_cluster_identifier ):
    #             raise ValueError("Cluster identifier does not exist")

    #         cluster_to_start = sql_commands.return_object_by_identifier(conn, db_cluster_identifier)
    #         cluster_to_start.start_cluster(conn)
    #         cluster_to_start.status = 'available'
    #         self.objects["Clusters"][db_cluster_identifier] = cluster_to_start
    #         cluster_to_start.save_changes_in_management_db(conn, True)
    #         return cluster_to_start.get_cluster_data_in_dict()
        
    def DescribeDBClusters(self, **kwargs):
         result = []
         with self.open_connection() as conn:
            if "max_records" in kwargs :
                if not validations.is_valid_number(kwargs["max_records"], 20, 100):
                     raise ValueError(f"Invalid max_records. max_records must be between 20 to 100.")
                else:
                    kwargs["max_records"] = 100
            if "db_cluster_identifier" in kwargs:
                if not validations.exist_value_in_column(conn, "object_management", "object_id", kwargs["db_cluster_identifier"] ):
                    raise ValueError("Cluster identifier does not exist")
                for key, inner_dict in self.objects.items():
                    if kwargs["db_cluster_identifier"] in inner_dict:
                        cluster_to_describe = inner_dict[kwargs["db_cluster_identifier"]]
                del kwargs["db_cluster_identifier"]
                result.append(cluster_to_describe.describe_cluster(conn, **kwargs))
            else:

                for cluster in self.objects["Clusters"].values():

                    passed_all_filters = True

                    # Check if the DB cluster matches all filter criteria
                    if "filters" in kwargs and "db-cluster-id" in kwargs["filters"] and cluster.db_cluster_identifier not in kwargs["filters"]["db-cluster-id"]:
                        passed_all_filters = False

                    if "filters" in kwargs and "engine" in kwargs["filters"] and cluster.engine not in kwargs["filters"]["engine"]:
                        passed_all_filters = False

                # Send the DB cluster to describe_cluster function if it passed all filters
                    if passed_all_filters:
                        result.append(cluster.describe_cluster(conn, **kwargs))
           
            return result
