from typing import Optional, Dict, List
import sqlite3
import Cluster
import validations

class Sara_Lea_Marx_Functions:
    
    def __init__(self, db_file: str):
        self.db_file = db_file
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
                metadata TEXT
            )
            ''')
            conn.commit()
    
    def validate_cluster_parameters(self, **kwargs):
        required_params=["db_cluster_identifier","engine"]
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
        else:
            if kwargs["engine"]=="MySQL" or kwargs["engine"]=="Aurora MySQL": 
                kwargs["port"]=3306
            elif kwargs["engine"]=="PostgreSQL" or kwargs["engine"]=="Aurora PostgreSQL":
                kwargs["port"]=5432
        if "storage_type" in kwargs.keys():
            if kwargs["storage_type"]!= "aurora-iopt1" and kwargs["storage_type"]!= "aurora":
                raise ValueError("Invalide storage_type value")
        else:
            kwargs["storage_type"] = "aurora"
        
        if validations.exist_key_value_in_json_column(conn, "object_management", "object_id", "db_cluster_identifier", kwargs["db_cluster_identifier"] ):
            raise ValueError("Cluster identifier already exist")

    def CreateDBCluster(self, **kwargs) -> str:
        """Create a new cluster and insert it into the object_management table."""
        if not self.validate_cluster_parameters(**kwargs):
            raise ValueError(f"Missing required fields")
        if not self.is_valid_dbClusterIdentifier(kwargs["db_cluster_identifier"]):
            raise ValueError(f"Invalid dbClusterIdentifier:")
        if not self.is_valid_engineName(kwargs["engine"]):
            raise ValueError(f"Invalid engineName: ")
        # self.check_parameters_constarins(**kwargs)
        with self.open_connection() as conn:
            self.check_parameters_constarins(conn, **kwargs)
            cluster = Cluster.Cluster(**kwargs)
            cluster.save_changes_in_management_db(conn)
            return {cluster: cluster.get_cluster_data_in_dict()}

