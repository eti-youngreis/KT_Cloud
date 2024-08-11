from DataAccess import GenericDataAccess
from typing import Dict, Any

class DataAccessLayer(GenericDataAccess):
    def insert(self, table_name: str, metadata: Dict[str, Any]) -> None:
        """Insert a new record into the specified table."""
        super().insert(table_name, 'DefaultType', metadata)

    def update(self, table_name: str, object_id: int, metadata: Dict[str, Any]) -> None:
        """Update an existing record in the specified table."""
        super().update(table_name, object_id, metadata)

    def select(self, table_name: str, criteria: str) -> Dict[str, Any]:
        """Select records from the specified table based on criteria."""
        return super().select(table_name, criteria)
 
    def delete(self, table_name: str, object_id: int) -> None:
        """Delete a record from the specified table."""
        super().delete(table_name, object_id)