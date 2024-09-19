from abc import abstractmethod
from typing import Dict, Optional, List
from NEW_KT_DB.DataAccess.ObjectManager import ObjectManager


class DBClusterParameterGroup:

    pk_column = 'group_name'
    table_structure = """
        group_name TEXT PRIMARY KEY,
        group_family TEXT,
        description TEXT,
        parameters TEXT 
        """
    def __init__(self, group_name: str, group_family: str, description: Optional[str] = None, tags: Optional[List[str]] = None, pk_column: str='DBClusterParameterGroupName', pk_value: str= None ):
        self.group_name = group_name
        self.group_family = group_family
        self.description = description
        self.parameters = self.load_default_parameters()
        self.tags = tags
        self.pk_column = pk_column
        self.pk_value = pk_value
    
    def load_default_parameters(self):
        """
        Loads default parameters for the DB parameter group.

        Returns:
        list: Default parameters for the DB parameter group
        """
        # Loading default parameters - can be replaced with actual parameters
        parameters = []
        parameters.append(Parameter('backup_retention_period', 7).to_dict())
        parameters.append(Parameter('preferred_backup_window', '03:00-03:30').to_dict())
        parameters.append(Parameter('preferred_maintenance_window', 'Mon:00:00-Mon:00:30').to_dict())
        return parameters
        

    def to_dict(self) -> Dict:
        return ObjectManager.convert_object_attributes_to_dictionary(
            group_name= self.group_name,
            group_family= self.group_family,
            description= self.description,
            parameters= self.parameters,
            # tags= self.tags,
            # pk_column=self.pk_column,
            # pk_value=self.pk_value
        )
    @staticmethod 
    def get_object_name():
        return DBClusterParameterGroup.__name__

from typing import Optional, List, Dict

class Parameter:
    def __init__(self, parameter_name: str, parameter_value: str, description: str = '', source: str = 'engine-default', apply_method: str = '', is_modifiable: bool = True):#, apply_type: str = '', data_type: str = '', allowed_values: str = '', is_modifiable: bool = True, minimum_engine_version: str = '', apply_method: str = '', supported_engine_modes: Optional[List[str]] = None):
        self.parameter_name = parameter_name
        self.parameter_value = parameter_value
        self.description = description
        # self.source = source
        # self.apply_type = apply_type
        # self.data_type = data_type
        # self.allowed_values = allowed_values
        self.is_modifiable = is_modifiable
        # self.minimum_engine_version = minimum_engine_version
        self.apply_method = apply_method
        # self.supported_engine_modes = supported_engine_modes

    def to_dict(self) -> Dict:
        return ObjectManager.convert_object_attributes_to_dictionary(
            parameter_name= self.parameter_name,
            parameter_value= self.parameter_value,
            description= self.description,
            # source= self.source,
            # apply_type=self.apply_type,
            # data_type= self.data_type,
            # allowed_values=self.allowed_values,
            is_modifiable= self.is_modifiable,
            # minimum_engine_version= self.minimum_engine_version,
            apply_method= self.apply_method
            # supported_engine_modes= self.supported_engine_modes
        )
