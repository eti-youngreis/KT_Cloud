from typing import Optional, Dict
import re
import sqlite3
import json

def create_connection(db_file: str) -> sqlite3.Connection:
    """Create and return a new SQLite database connection."""
    return sqlite3.connect(db_file)

def create_table(conn: sqlite3.Connection) -> None:
    """Create the object_management table if it does not exist."""
    c = conn.cursor()
    c.execute('''
    CREATE TABLE IF NOT EXISTS object_management (
        object_id INTEGER PRIMARY KEY,
        type_object TEXT,
        metadata TEXT
    )
    ''')
    conn.commit()

def is_valid_engineName(engineName: str) -> bool:
    """Check if the engineName is valid."""
    valid_engineNames = {
        "db2-ae", "db2-se", "mariadb", "mysql", "oracle-ee",
        "oracle-ee-cdb", "oracle-se2", "oracle-se2-cdb",
        "postgres", "sqlserver-ee", "sqlserver-se",
        "sqlserver-ex", "sqlserver-web"
    }
    return engineName in valid_engineNames

def is_valid_optionGroupName(optionGroupName: str) -> bool:
    """Check if the optionGroupName is valid based on the pattern and length."""
    pattern = r'^[a-zA-Z][a-zA-Z0-9]*(?:-[a-zA-Z0-9]+)*[a-zA-Z0-9]$'
    return bool(re.match(pattern, optionGroupName)) and 1 <= len(optionGroupName) <= 255

def option_group_exists(conn: sqlite3.Connection, optionGroupName: str) -> bool:
    """Check if an option group with the given name already exists."""
    c = conn.cursor()
    c.execute('''
    SELECT COUNT(*) FROM object_management
    WHERE metadata LIKE ?
    ''', (f'%"optionGroupName": "{optionGroupName}"%',))
    return c.fetchone()[0] > 0

def get_option_group_count(conn: sqlite3.Connection) -> int:
    """Get the number of option groups in the table."""
    c = conn.cursor()
    c.execute('''
    SELECT COUNT(*) FROM object_management
    WHERE type_object = 'OptionGroup'
    ''')
    return c.fetchone()[0]

def insert_new_optionGroup_to_object_management_table(
    conn: sqlite3.Connection, 
    engineName: str, 
    majorEngineVersion: str, 
    optionGroupDescription: str, 
    optionGroupName: str, 
    tags: Optional[Dict] = None
) -> None:
    """Insert a new option group into the object_management table."""
    if option_group_exists(conn, optionGroupName):
        raise ValueError(f"Option group '{optionGroupName}' already exists.")
    if get_option_group_count(conn) >= 20:
        raise ValueError("Quota of 20 option groups exceeded for this AWS account.")

    metadata_dict = {
        "engineName": engineName,
        "majorEngineVersion": majorEngineVersion,
        "optionGroupDescription": optionGroupDescription,
        "optionGroupName": optionGroupName
    }
    
    if tags is not None:
        metadata_dict["tags"] = tags
    
    metadata = json.dumps(metadata_dict)
    
    c = conn.cursor()
    c.execute('''
    INSERT INTO object_management (type_object, metadata)
    VALUES (?, ?)
    ''', ('OptionGroup', metadata))

    conn.commit()

def CreateOptionGroup(
    conn: sqlite3.Connection,
    engineName: str, 
    majorEngineVersion: str, 
    optionGroupDescription: str, 
    optionGroupName: str,
    tags: Optional[Dict] = None
) -> None:
    """Create a new option group and insert it into the object_management table."""
    if not is_valid_engineName(engineName):
        raise ValueError(f"Invalid engineName: {engineName}")
    if not is_valid_optionGroupName(optionGroupName):
        raise ValueError(f"Invalid optionGroupName: {optionGroupName}")
    
    insert_new_optionGroup_to_object_management_table(
        conn, engineName, majorEngineVersion, optionGroupDescription, optionGroupName, tags
    )

# Example usage
if __name__ == "__main__":
    db_file = 'object_management_db.db'
    conn = create_connection(db_file)
    create_table(conn)
    
    try:
        CreateOptionGroup(
            conn,
            "mysql", 
            "5.7", 
            "My option group description", 
            "my_option_group", 
            tags={"key": "value"}
        )
    except ValueError as e:
        print(f"Error: {e}")
    finally:
        conn.close()
