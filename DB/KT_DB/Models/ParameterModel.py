from typing import Optional, List, Dict

class ParameterModel:
    def __init__(self, parameter_name: str, parameter_value: str, description: str = '', source: str = 'engine-default', apply_type: str = '', data_type: str = '', allowed_values: str = '', is_modifiable: bool = True, minimum_engine_version: str = '', apply_method: str = '', supported_engine_modes: Optional[List[str]] = None):
        self.parameter_name = parameter_name
        self.parameter_value = parameter_value
        self.description = description
        self.source = source
        self.apply_type = apply_type
        self.data_type = data_type
        self.allowed_values = allowed_values
        self.is_modifiable = is_modifiable
        self.minimum_engine_version = minimum_engine_version
        self.apply_method = apply_method
        self.supported_engine_modes = supported_engine_modes

    def to_dict(self) -> Dict:
        return {
            'ParameterName': self.parameter_name,
            'ParameterValue': self.parameter_value,
            'Description': self.description,
            'Source': self.source,
            'ApplyType': self.apply_type,
            'DataType': self.data_type,
            'AllowedValues': self.allowed_values,
            'IsModifiable': self.is_modifiable,
            'MinimumEngineVersion': self.minimum_engine_version,
            'ApplyMethod': self.apply_method,
            'SupportedEngineModes': self.supported_engine_modes
        }