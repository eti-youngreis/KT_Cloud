from typing import Optional, Dict, List
import json
import validations
import sqlite3
from sqlite3 import OperationalError
from typing import Optional, Dict

def is_valid_db_security_group_name(db_security_group_name: str) -> bool:
    """
    Check if the db_security_group_name is valid based on the pattern and length.

    The db_security_group_name must:
    - Be between 1 and 255 characters in length.
    - Start with a letter.
    - Contain only letters, digits, or hyphens.
    - Not end with a hyphen.
    - Not contain two consecutive hyphens.
    - Not be "Default".

    Args:
        db_security_group_name (str): The name of the DB security group to validate.

    Returns:
        bool: True if the db_security_group_name is valid, False otherwise.
    """
    pattern = r"^(?!Default$)([a-zA-Z][a-zA-Z0-9]*)(-?([a-zA-Z0-9]+))*[a-zA-Z0-9]$"
    return validations.is_valid_length(db_security_group_name, 1, 255) and validations.is_valid_pattern(db_security_group_name, pattern)

def db_security_group_name_exists(conn: sqlite3.Connection, db_security_group_name: str) -> bool:
    """Check if an db_security_group with the given name already exists."""
    return validations.exist_key_value_in_json_column(conn, "object_management", "metadata", "DBSecurityGroupName", db_security_group_name)

def insert_new_db_security_group_to_object_management_table(
    conn: sqlite3.Connection,
    db_security_group_description: str, 
    db_security_group_name: str,
    tags: Optional[Dict] = None
) -> None:
    """Insert a new db_security_group into the object_management table."""

    metadata_dict = {
        "DBSecurityGroupDescription": db_security_group_description,
        "DBSecurityGroupName": db_security_group_name,
        "available": 'True'
    }
    
    if tags is not None:
        metadata_dict["tags"] = tags
    
    metadata = json.dumps(metadata_dict)
    
    try:
        c = conn.cursor()
        c.execute('''
        INSERT INTO object_management (type_object, metadata)
        VALUES (?, ?)
        ''', ('DBSecurityGroup', metadata))

        conn.commit()
    except OperationalError as e:
        print(f"Error: {e}")

def is_db_security_group_available(conn: sqlite3.Connection, db_security_group_name: str) -> bool:
    """Check if an db_security_group with the specified name and availability status exists."""
    c = conn.cursor()
    c.execute('''
    SELECT metadata FROM object_management
    WHERE type_object = 'DBSecurityGroup'
    ''')
    
    rows = c.fetchall()
    for row in rows:
        metadata = json.loads(row[0])
        if (metadata.get("DBSecurityGroupName") == db_security_group_name and 
                metadata.get("available") == 'True'):
            return True
    
    return False

def delete_db_security_group_name_from_object_management_table(conn: sqlite3.Connection, db_security_group_name: str) -> None:
    """Delete db_security_group_name from object management table."""
    try:
        c = conn.cursor()
        c.execute(f'''
        DELETE FROM object_management
        WHERE type_object = ? AND metadata LIKE ?
        ''', ('DBSecurityGroup', f'%"DBSecurityGroupName": "{db_security_group_name}"%',))

        conn.commit()
    except OperationalError as e:
        print(f"Error: {e}")

def delete_db_security_group_name_from_DBInstance_metadata(conn: sqlite3.Connection, db_security_group_name: str) -> None:
    """Delete the 'db_security_group_name' key and its value from metadata where type_object is 'DBInstance' and the value equals db_security_group_name."""
    try:
        c = conn.cursor()
        c.execute('''
        SELECT object_id, metadata FROM object_management 
        WHERE type_object = ?
        ''', ('DBInstance',))
        
        rows = c.fetchall()
        
        for row in rows:
            object_id = row[0]
            metadata = json.loads(row[1])
            
            if 'DBSecurityGroupName' in metadata and metadata['DBSecurityGroupName'] == db_security_group_name:
                del metadata['DBSecurityGroupName']
                updated_metadata = json.dumps(metadata)
                
                c.execute('''
                UPDATE object_management 
                SET metadata = ?
                WHERE object_id = ?
                ''', (updated_metadata, object_id))
        
        conn.commit()
    except sqlite3.OperationalError as e:
        print(f"Error: {e}")

def CreateDBSecurityGroup(
        conn: sqlite3.Connection,
        db_security_group_description: str,
        db_security_group_name: str,
        tags: Optional[Dict] = None
    )->str:
        """Create a new security group and insert it into the object_management table."""
        if not is_valid_db_security_group_name(db_security_group_name):
            raise ValueError(f"Invalid db_security_group_name: {db_security_group_name}")
        if db_security_group_name_exists(conn, db_security_group_name):
            return {
                "Error": {
                    "Code": "securityGroupAlreadyExistsFault",
                    "Message": f"db_security_group_name '{db_security_group_name}' already exists.",
                    "HTTPStatusCode": 400
                }
            }
        try:
            insert_new_db_security_group_to_object_management_table(
                conn, db_security_group_description, db_security_group_name, tags
            )
            return{
                "CreateDBSecurityGroupResponse": {
                    "ResponseMetadata": {
                        "HTTPStatusCode": 200
                    }
                }
            }
        except OperationalError as e:
            return {
                "Error": {
                    "Code": "InternalError",
                    "Message": f"An internal error occurred: {str(e)}",
                    "HTTPStatusCode": 500
                }
            }
        
def DeleteDBSecurityGroup(conn: sqlite3.Connection, db_security_group_name: str) -> str:
    """
    Deletes an existing security group.
    The function checks if the security group exists and if it is in the 'available' state.
    If the security group does not exist or is not available, the function returns an appropriate error message in JSON format.
    If the security group is successfully deleted, the function returns a success response in JSON format.
    """
    if not db_security_group_name_exists(conn, db_security_group_name):
        return {
            "Error": {
                "Code": "SecurityGroupNotFoundFault",
                "Message": f"The specified db_security_group_name_exists '{db_security_group_name}' could not be found.",
                "HTTPStatusCode": 404
            }
        }

    if not is_db_security_group_available(conn, db_security_group_name):
        return {
            "Error": {
                "Code": "InvalidSecurityGroupStateFault",
                "Message": f"The db_security_group_name '{db_security_group_name}' isn't in the available state.",
                "HTTPStatusCode": 400
            }
        }
    try:
        delete_db_security_group_name_from_DBInstance_metadata(conn, db_security_group_name)
        delete_db_security_group_name_from_object_management_table(conn, db_security_group_name)
        return{
            "DeleteSecurityGroupResponse": {
                "ResponseMetadata": {
                    "HTTPStatusCode": 200
                }
            }
        }
    except OperationalError as e:
        return {
            "Error": {
                "Code": "InternalError",
                "Message": f"An internal error occurred: {str(e)}",
                "HTTPStatusCode": 500
            }
        }
  
def DescribeDBSecurityGroups(
        conn: sqlite3.Connection,
        db_security_group_name: Optional[str] = None,
        marker: Optional[str] = None,
        max_records: int = 100    
    ) -> dict:
        """Describes the available db security groups."""
        if db_security_group_name is not None:
            if not db_security_group_name_exists(conn, db_security_group_name):
                return {
                    "Error": {
                        "Code": "SecurityGroupNotFoundFault",
                        "Message": f"The specified db_security_group_name_exists '{db_security_group_name}' could not be found.",
                        "HTTPStatusCode": 404
                    }
                }
        query = '''
        SELECT object_id, metadata FROM object_management 
        WHERE type_object = 'DBSecurityGroup'
        '''
        params = []

        if db_security_group_name:
            query += ' AND metadata LIKE ?'
            params.append(f'%"{db_security_group_name}"%')

        query += ' ORDER BY object_id LIMIT ? OFFSET ?'
        offset = int(marker) if marker else 0
        params.append(max_records)
        params.append(offset)

        try:
            c = conn.cursor()
            c.execute(query, params)
            rows = c.fetchall()

            db_security_groups_list = []
            for row in rows:
                metadata = json.loads(row[1])
                db_security_groups_list.append(metadata)

            next_marker = offset + max_records if len(rows) == max_records else None

            return {
                "DescribeDBSecurityGroupResponse": {
                    "ResponseMetadata": {
                        "HTTPStatusCode": 200
                    },
                    "DBSecurityGroupList": db_security_groups_list,
                    "Marker": next_marker
                }
            }
        except OperationalError as e:
            return {
                "Error": {
                    "Code": "InternalError",
                    "Message": f"An internal error occurred: {str(e)}",
                    "HTTPStatusCode": 500
                }
            }
