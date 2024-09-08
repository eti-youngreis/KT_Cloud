from abc import ABC, abstractmethod

class STO(ABC):
    @abstractmethod
    def create(self, *args, **kwargs):
        """Create a new storage object."""
        pass

    @abstractmethod
    def delete(self, *args, **kwargs):
        """Delete an existing storage object."""
        pass

    @abstractmethod
    def get(self, *args, **kwargs):
        """get storage object."""
        pass

    @abstractmethod
    def put(self, *args, **kwargs):
        """put storage object."""
        pass
