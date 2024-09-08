from .STO import STO
from abc import ABC, abstractmethod

class STOE(STO):
    @abstractmethod
    def list(self, *args, **kwargs):
        """list storage object."""
        pass

    @abstractmethod
    def head(self, *args, **kwargs):
        """check if object exists and is accessible with the appropriate user permissions."""
        pass
