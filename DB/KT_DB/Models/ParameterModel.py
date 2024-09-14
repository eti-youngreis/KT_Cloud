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
            'parameter_name': self.parameter_name,
            'parameter_value': self.parameter_value,
            'description': self.description,
            'source': self.source,
            'apply_type': self.apply_type,
            'data_type': self.data_type,
            'allowed_values': self.allowed_values,
            'is_modifiable': self.is_modifiable,
            'minimum_engine_version': self.minimum_engine_version,
            'apply_method': self.apply_method,
            'supported_engine_modes': self.supported_engine_modes
        }