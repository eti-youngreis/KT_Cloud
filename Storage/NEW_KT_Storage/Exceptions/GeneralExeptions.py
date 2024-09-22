
class ObjectNotFoundException(Exception):
    def __init__(self, object_name: str):
        self.object_name = object_name
        super().__init__(f'{self.object_name} not found.')