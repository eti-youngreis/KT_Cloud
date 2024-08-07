class Parameter:
    def __init__(self, parameter_name, parameter_value, description='', source='engine-default', apply_type='',
                 data_type='', allowed_values='', is_modifiable=True, minimum_engine_version='',
                 apply_method='', supported_engine_modes=None):
        """
        Initializes a Parameter instance.

        Args:
        parameter_name (str): The name of the parameter.
        parameter_value (str): The value of the parameter.
        description (str, optional): A description of the parameter (default: '').
        source (str, optional): The source of the parameter (default: '').
        apply_type (str, optional): The apply type of the parameter (default: '').
        data_type (str, optional): The data type of the parameter (default: '').
        allowed_values (str, optional): The allowed values for the parameter (default: '').
        is_modifiable (bool, optional): Whether the parameter is modifiable (default: True).
        minimum_engine_version (str, optional): The minimum engine version required (default: '').
        apply_method (str, optional): The apply method for the parameter (default: '').
        supported_engine_modes (list, optional): The supported engine modes (default: None).
        """
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
        self.supported_engine_modes = supported_engine_modes if supported_engine_modes is not None else []

    def describe(self):
        """
        Converts the Parameter instance to a dictionary.

        Returns:
        dict: A dictionary representation of the parameter.
        """
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

    def update(self, new_parameter):
        if self.is_modifiable == False:
            raise ValueError(f"you can't modify the parameter {
                             self.parameter_name}")
        self.source = 'user'
        if 'ParameterValue' in new_parameter:
            self.parameter_name = new_parameter['ParameterValue']
        if 'Description' in new_parameter:
            self.description = new_parameter['Description']
        if 'IsModifiable' in new_parameter:
            self.is_modifiable = new_parameter['IsModifiable']

# דוגמה לשימוש במחלקה
# param = Parameter(
#     parameter_name='max_connections',
#     parameter_value='150',
#     description='Maximum number of connections',
#     source='user',
#     apply_type='dynamic',
#     data_type='integer',
#     allowed_values='1-10000',
#     is_modifiable=True,
#     minimum_engine_version='10.1',
#     apply_method='immediate',
#     supported_engine_modes=['provisioned']
# )

# print(param.to_dict())
