from typing import Dict, Optional
from DataAccess import ClusterManager
from Models import DBClusterModel
from Abc import DBO
from Validation import Validation 


class DBClusterService(DBO):
    def __init__(self, dal: ClusterManager):
        self.dal = dal
    
    def validate_cluster_parameters(self,required_params, **kwargs):
        return Validation.check_required_params(required_params,**kwargs)
        
    def is_valid_dbClusterIdentifier(self, dbClusterIdentifier: str) -> bool:
        """Check if the bdClusterIdentifier is valid based on the pattern and length."""
        pattern = r'^[a-zA-Z][a-zA-Z0-9]*(?:-[a-zA-Z0-9]+)*[a-zA-Z0-9]$'
        return Validation.is_valid_length(dbClusterIdentifier, 1, 63) and Validation.is_valid_pattern(dbClusterIdentifier, pattern)

    def is_valid_engineName(self, engine_name: str) -> bool:
        """Check if the engineName is valid."""
        valid_engine_names = {
          "neptune", "postgres", "mysql", "aurora-postgresql", "aurora-mysql"
        }
        return Validation.string_in_dict(engine_name, valid_engine_names)
    
    def is_identifier_exit(self, db_cluster_identifier):
        return self.dal.is_identifier_exit(db_cluster_identifier)
    
    def check_parameters_constarins(self, conn, **kwargs):
        """checks all the cluster parameters constrains"""

        if "db_cluster_parameter_group_name" in kwargs.keys():
            if not self.dal.exist_key_value_in_json_column("db_cluster_parameter_group_name", kwargs["db_cluster_parameter_group_name"] ):
                raise ValueError(f"Parameter group name does not exist")
        
        if "db_subnet_group_name" in kwargs.keys():
            if not self.dal.exist_key_value_in_json_column("db_subnet_group_name", kwargs["db_subnet_group_name"] ):
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


    def create(self, **kwargs) -> Dict:
        """
        Create a new database cluster and store it in the database.

        Parameters:
        - db_cluster_identifier (str): The unique identifier for the cluster.
        - engine (str): The database engine to use (e.g., "mysql", "postgres").
        - other_params: Additional parameters for cluster configuration.

        Returns:
        - Dict: A dictionary representation of the created cluster.

        Raises:
        - ValueError: If required parameters are missing, or values are invalid.
        """
        required_params = ["db_cluster_identifier", "engine"]
        if not self.validate_cluster_parameters(required_params, **kwargs):
            raise ValueError("Missing required fields")

        db_cluster_identifier = kwargs["db_cluster_identifier"]
        engine = kwargs["engine"]

        if not self.is_valid_dbClusterIdentifier(db_cluster_identifier):
            raise ValueError(f"Invalid dbClusterIdentifier: {db_cluster_identifier}")
        
        if not self.is_valid_engineName(engine):
            raise ValueError(f"Invalid engineName: {engine}")

        if self.is_identifier_exit(db_cluster_identifier):
            raise ValueError("Cluster identifier already exists")

        self.check_parameters_constraints(**kwargs)

        cluster = DBClusterModel(**kwargs)
        
        try:
            self.dal.create(cluster.to_dict())
        except Exception as e:
            # Handle any exceptions that occur during the database operation
            raise RuntimeError(f"Failed to create cluster: {e}")

        return cluster.to_dict()


    def modify(self, **kwargs) -> Dict:
        """Modify an existing db cluster"""
        pass

    def describe(self, **kwargs) -> Dict:
        """Retrieve the details of a cluster."""
        pass

    def delete(self, **kwargs):
        """Delete an existing db cluster"""
        pass
