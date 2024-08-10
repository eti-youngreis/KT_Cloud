import DBO
from abc import ABC, abstractmethod

class Snapshot(DBO):
    @abstractmethod
    def copy(self, *args, **kwargs):
        """Copy the snapshot."""
        pass

    @abstractmethod
    def restore(self, *args, **kwargs):
        """Restore from the snapshot."""
        pass
