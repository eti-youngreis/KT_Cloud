from typing import Dict, Optional, List
from Models import ParameterModel
from Models import ParameterGroupModel

class DBParameterGroupModel(ParameterGroupModel):
    def __init__(self, group_name: str, group_family: str, description: Optional[str] = None, tags: Optional[List[str]] = None):
        super().__init__(group_name, group_family, description, tags)
        
    def load_default_parameters(self):
        """
        Loads default parameters for the DB parameter group.

        Returns:
        list: Default parameters for the DB parameter group
        """
        # Loading default parameters - can be replaced with actual parameters
        parameters = []
        parameters.append(ParameterModel('max_connections', 100))
        parameters.append(ParameterModel('innodb_buffer_pool_size', '128M'))
        parameters.append(ParameterModel('character_set_server', 'utf8'))
        parameters.append(ParameterModel('time_zone', 'UTC'))
        # for p in parameters:
        #     p.save_to_db(conn)
        return parameters
        