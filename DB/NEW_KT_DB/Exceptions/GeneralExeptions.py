
class ObjectNotFoundException(Exception):
    def __init__(self, object_type:str, object_name: str):
        self.object_type = object_type
        self.object_name = object_name
        super().__init__(f'{self.object_type}: {self.object_name} not found.')

class InvalidObjectStateException(Exception):
    def __init__(self, object_type:str, object_name: str, valid_state:str, object_state:str):
        self.object_type = object_type
        self.object_name = object_name
        self.valid_state = valid_state
        self.object_state = object_state
        super().__init__(f'invalid {self.object_type} {self.object_name} state.The valid state is {self.valid_state}, object state is {self.object_state}')

class ObjectAlreadyExistsException(Exception):
    def __init__(self, object_type:str, object_name: str):
        self.object_type = object_type
        self.object_name = object_name
        super().__init__(f'{self.object_type}: {self.object_name} already exists.')

class InvalidParamException(Exception):
    def __init__(self, param_name, param_value, param_valid_state):
        self.param_name = param_name
        self.param_value = param_value
        self.param_valid_state = param_valid_state
        super().__init__(f'{self.param_name}: {self.param_value} is invalid, {self.param_name} should be {self.param_valid_state}')