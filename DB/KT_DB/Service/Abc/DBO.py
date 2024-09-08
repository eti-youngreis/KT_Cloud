from abc import ABC, abstractmethod

class DBO(ABC):
    @abstractmethod
    def create(self, *args, **kwargs):
        '''Create a new database object.'''
        pass

    @abstractmethod
    def delete(self, *args, **kwargs):
        '''Delete an existing database object.'''
        pass

    @abstractmethod
    def describe(self, *args, **kwargs):
        '''Describe the details of a database object.'''
        pass

    @abstractmethod
    def modify(self, *args, **kwargs):
        '''Modify an existing database object.'''
        pass
