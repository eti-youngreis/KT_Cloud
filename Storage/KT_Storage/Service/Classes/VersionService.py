from typing import Dict, Optional
from DataAccess import VersionManager
# from ...Models.VesionModel import Version
from Abc import STO

class VersionService(STO):
    def __init__(self, dal: VersionManager):
        self.dal = dal

    def create(self, bucket_name: str, major_bucket_version: str, version_content: str, version_name: str, tags: Optional[Dict] = None):
        """Create a new version."""
        pass

    def delete(self, version_id: str):
        """Delete an existing Version."""
        pass

    def get(self, version_name: str) -> Dict:
        """Get a Version."""
        pass

    def put(self, version_name: str, updates: Dict):
        """Put a Version."""
        pass