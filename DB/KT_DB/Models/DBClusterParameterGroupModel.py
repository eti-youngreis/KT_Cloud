from typing import Dict, Optional, List
from Models import ParameterModel
from Models import ParameterGroupModel

class DBClusterParameterGroupModel(ParameterGroupModel):
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
        parameters.append(ParameterModel('backup_retention_period', 7))
        parameters.append(ParameterModel('preferred_backup_window', '03:00-03:30'))
        parameters.append(ParameterModel('preferred_maintenance_window', 'Mon:00:00-Mon:00:30'))
        # for p in parameters:
        #     p.save_to_db(conn)
        return parameters
        