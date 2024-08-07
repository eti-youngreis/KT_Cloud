from typing import Optional, Dict, List
import sqlite3
import OptionGroup
import DBSecurityGroup
import DBSubnetGroup

class Gili_Bolak_Functions:
    
    def __init__(self, db_file: str):
        self.db_file = db_file
        self.create_object_management_table()

    def open_connection(self) -> sqlite3.Connection:
        """Initialize the SQLite connection."""
        return sqlite3.connect(self.db_file)

    def create_object_management_table(self) -> None:
        """Create the object_management table if it does not exist."""
        conn = self.open_connection()
        c = conn.cursor()
        c.execute('''
        CREATE TABLE IF NOT EXISTS object_management (
            object_id INTEGER PRIMARY KEY,
            type_object TEXT,
            metadata TEXT
        )
        ''')
        conn.commit()
        conn.close()

    def CreateOptionGroup(
        self,
        engine_name: str, 
        major_engine_version: str, 
        option_group_description: str, 
        option_group_name: str,
        tags: Optional[Dict] = None
    ) -> str:
        """Create a new option group and insert it into the object_management table."""
        conn = self.open_connection()
        return_obj = OptionGroup.CreateOptionGroup(conn, engine_name, major_engine_version, option_group_description, option_group_name, tags)
        conn.close()
        return return_obj

    def DeleteOptionGroup(self, option_group_name: str) -> str:
        """
        Deletes an existing option group.
        The function checks if the option group exists and if it is in the 'available' state.
        If the option group does not exist or is not available, the function returns an appropriate error message in JSON format.
        If the option group is successfully deleted, the function returns a success response in JSON format.
        """
        conn = self.open_connection()
        return_obj = OptionGroup.DeleteOptionGroup(conn, option_group_name)
        conn.close()
        return return_obj

    def ModifyOptionGroup(
        self,
        option_group_name: str,
        apply_immediately: bool = False,
        options_to_include: Optional[List[Dict]] = None,
        options_to_remove: Optional[List[str]] = None
    ) -> str:
        """Modify Option Group."""
        conn = self.open_connection()
        return_obj = OptionGroup.ModifyOptionGroup(conn, option_group_name, apply_immediately, options_to_include, options_to_remove)
        conn.close()
        return return_obj

    def CopyOptionGroup(
        self,
        source_option_group_identifier: str,
        target_option_group_description: str,
        target_option_group_identifier: str,
        tags: Optional[Dict] = None
    ) -> str:
        """Copy an existing option group and insert it into the object_management table."""
        conn = self.open_connection()
        return_obj = OptionGroup.CopyOptionGroup(conn, source_option_group_identifier, target_option_group_description, target_option_group_identifier, tags)
        conn.close()
        return return_obj
    
    def DescribeOptionGroups(
        self,
        engine_name: Optional[str] = None,
        major_engine_version: Optional[str] = None,
        marker: Optional[str] = None,
        max_records: int = 100,
        option_group_name: Optional[str] = None
    ) -> dict:
        """Describes the available option groups."""
        conn = self.open_connection()
        return_obj = OptionGroup.DescribeOptionGroups(conn, engine_name, major_engine_version, marker, max_records, option_group_name)
        conn.close()
        return return_obj

    def DescribeOptionGroupOptions(
        self,
        engine_name: str,
        major_engine_version: Optional[str] = None,
        marker: Optional[str] = None,
        max_records: int = 100
    ) -> dict:
        """Describes all available options for the specified engine."""
        conn = self.open_connection()
        return_obj = OptionGroup.DescribeOptionGroupOptions(conn, engine_name, major_engine_version, marker, max_records)
        conn.close()
        return return_obj
    
    def CreateDBSecurityGroup(
        self,
        db_security_group_description: str,
        db_security_group_name: str,
        tags: Optional[Dict] = None
    )->str:
        """Create a new db_security_group and insert it into the object_management table."""
        conn = self.open_connection()
        return_obj = DBSecurityGroup.CreateDBSecurityGroup(conn, db_security_group_description, db_security_group_name, tags)
        conn.close()
        return return_obj
    
    def DeleteDBSecurityGroup(self, db_security_group_name: str) -> str:
        """
        Deletes an existing db_security_group.
        The function checks if the db_security_group exists and if it is in the 'available' state.
        If the db_security_group does not exist or is not available, the function returns an appropriate error message in JSON format.
        If the db_security_group is successfully deleted, the function returns a success response in JSON format.
        """
        conn = self.open_connection()
        return_obj = DBSecurityGroup.DeleteDBSecurityGroup(conn, db_security_group_name)
        conn.close()
        return return_obj
    
    def DescribeDBSecurityGroups(
        self,
        db_security_group_name: str = None,
        marker: Optional[str] = None,
        max_records: int = 100    
    ) -> dict:
        """Describes the available db security groups."""
        conn = self.open_connection()
        return_obj = DBSecurityGroup.DescribeDBSecurityGroups(conn, db_security_group_name, marker, max_records)
        conn.close()
        return return_obj
    
    def DescribeDBSubnetGroups(
        self,
        db_security_group_name: str = None,
        marker: Optional[str] = None,
        max_records: int = 100    
    ) -> dict:
        """Describes the available db subnet groups."""
        conn = self.open_connection()
        return_obj = DBSubnetGroup.DescribeDBSubnetGroups(conn, db_security_group_name, marker, max_records)
        conn.close()
        return return_obj