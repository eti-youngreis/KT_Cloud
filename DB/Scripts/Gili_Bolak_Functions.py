from typing import Optional, Dict, List
import sqlite3
import OptionGroup

class Gili_Bolak_Functions:
    
    def __init__(self, db_file: str):
        self.db_file = db_file
        self.create_object_management_table()

    def open_connection(self) -> sqlite3.Connection:
        """Initialize the SQLite connection."""
        return sqlite3.connect(self.db_file)

    def create_object_management_table(self) -> None:
        """Create the object_management table if it does not exist."""
        with self.open_connection() as conn:
            c = conn.cursor()
            c.execute('''
            CREATE TABLE IF NOT EXISTS object_management (
                object_id INTEGER PRIMARY KEY,
                type_object TEXT,
                metadata TEXT
            )
            ''')
            conn.commit()

    def CreateOptionGroup(
        self,
        engine_name: str, 
        major_engine_version: str, 
        option_group_description: str, 
        option_group_name: str,
        tags: Optional[Dict] = None
    ) -> str:
        """Create a new option group and insert it into the object_management table."""
        with self.open_connection() as conn:
            return OptionGroup.CreateOptionGroup(conn, engine_name, major_engine_version, option_group_description, option_group_name, tags)

    def DeleteOptionGroup(self, option_group_name: str) -> str:
        """
        Deletes an existing option group.
        The function checks if the option group exists and if it is in the 'available' state.
        If the option group does not exist or is not available, the function returns an appropriate error message in JSON format.
        If the option group is successfully deleted, the function returns a success response in JSON format.
        """
        with self.open_connection() as conn:
            return OptionGroup.DeleteOptionGroup(conn, option_group_name)

    def ModifyOptionGroup(
        self,
        option_group_name: str,
        apply_immediately: bool = False,
        options_to_include: Optional[List[Dict]] = None,
        options_to_remove: Optional[List[str]] = None
    ) -> str:
        """Modify Option Group."""
        with self.open_connection() as conn:
            return OptionGroup.ModifyOptionGroup(conn, option_group_name, apply_immediately, options_to_include, options_to_remove)

    def CopyOptionGroup(
        self,
        source_option_group_identifier: str,
        target_option_group_description: str,
        target_option_group_identifier: str,
        tags: Optional[Dict] = None
    ) -> str:
        """Copy an existing option group and insert it into the object_management table."""
        with self.open_connection() as conn:
            return OptionGroup.CopyOptionGroup(conn, source_option_group_identifier, target_option_group_description, target_option_group_identifier, tags)

    def DescribeOptionGroups(
        self,
        engine_name: Optional[str] = None,
        major_engine_version: Optional[str] = None,
        marker: Optional[str] = None,
        max_records: int = 100,
        option_group_name: Optional[str] = None
    ) -> dict:
        """Describes the available option groups."""
        with self.open_connection() as conn:
            return OptionGroup.DescribeOptionGroups(conn, engine_name, major_engine_version, marker, max_records, option_group_name)

    def DescribeOptionGroupOptions(
        self,
        engine_name: str,
        major_engine_version: Optional[str] = None,
        marker: Optional[str] = None,
        max_records: int = 100
    ) -> dict:
        """Describes all available options for the specified engine."""
        with self.open_connection() as conn:
            return OptionGroup.DescribeOptionGroupOptions(conn, engine_name, major_engine_version, marker, max_records)