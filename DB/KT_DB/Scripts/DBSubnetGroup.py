from typing import Optional
import json
import validations
import sqlite3
from sqlite3 import OperationalError

def db_subnet_group_name_exists(conn: sqlite3.Connection, db_subnet_group_name: str) -> bool:
    """Check if an db_subnet_group with the given name already exists."""
    return validations.exist_key_value_in_json_column(conn, "object_management", "metadata", "DBSubnetGroupName", db_subnet_group_name)

def DescribeDBSubnetGroups(
        conn: sqlite3.Connection,
        db_subnet_group_name: Optional[str] = None,
        marker: Optional[str] = None,
        max_records: int = 100    
    ) -> dict:
        """Describes the available db security groups."""
        if db_subnet_group_name is not None:
            if not db_subnet_group_name_exists(conn, db_subnet_group_name):
                return {
                    "Error": {
                        "Code": "DBSubnetGroupNotFoundFault",
                        "Message": f"The specified db_subnet_group_name '{db_subnet_group_name}' could not be found.",
                        "HTTPStatusCode": 404
                    }
                }
        query = '''
        SELECT object_id, metadata FROM object_management 
        WHERE type_object = 'DBSubnetGroup'
        '''
        params = []

        if db_subnet_group_name:
            query += ' AND metadata LIKE ?'
            params.append(f'%"{db_subnet_group_name}"%')

        query += ' ORDER BY object_id LIMIT ? OFFSET ?'
        offset = int(marker) if marker else 0
        params.append(max_records)
        params.append(offset)

        try:
            c = conn.cursor()
            c.execute(query, params)
            rows = c.fetchall()

            db_subnet_groups_list = []
            for row in rows:
                metadata = json.loads(row[1])
                db_subnet_groups_list.append(metadata)

            next_marker = offset + max_records if len(rows) == max_records else None

            return {
                "DescribeDBSubnetGroupResponse": {
                    "ResponseMetadata": {
                        "HTTPStatusCode": 200
                    },
                    "DBSubnetGroupList": db_subnet_groups_list,
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
