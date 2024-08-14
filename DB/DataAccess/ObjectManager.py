from typing import Dict, Any
from DBManager import DBManager

class ObjectManager:
    def __init__(self, db_file: str):
        """Initialize the ObjectManager with a database file."""
        self.db_manager = DBManager(db_file)

    def insert_object(self, object_type: str, metadata: Dict[str, Any]) -> None:
        """Insert a new object into the database."""
        self.db_manager.insert("objects", object_type, metadata)

    def update_object(self, object_id: int, metadata: Dict[str, Any]) -> None:
        """Update an existing object in the database."""
        self.db_manager.update("objects", {"metadata": json.dumps(metadata)}, f"object_id = {object_id}")

    def get_objects(self, criteria: str) -> Dict[int, Dict[str, Any]]:
        """Retrieve objects from the database based on criteria."""
        return self.db_manager.select("objects", criteria)

    def delete_object(self, object_id: int) -> None:
        """Delete an object from the database."""
        self.db_manager.delete("objects", f"object_id = {object_id}")

    def close(self):
        """Close the database connection."""
        self.db_manager.close()
