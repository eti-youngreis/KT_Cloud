import sqlite3
import os
from typing import List, Tuple
from DB.NEW_KT_DB.Exceptions.DBInstanceExceptions import InvalidQueryError, AlreadyExistsError, DatabaseCreationError


class SQLCommandManager:
    @staticmethod
    def clone_database_schema(source_db_path: str, new_db_path: str) -> None:
        source_conn = None
        new_conn = None
        try:
            source_conn = sqlite3.connect(source_db_path)
            source_cursor = source_conn.cursor()
    
            new_conn = sqlite3.connect(new_db_path)
            new_cursor = new_conn.cursor()
    
            source_cursor.execute(
                "SELECT sql FROM sqlite_master WHERE type='table'")
            tables = source_cursor.fetchall()
    
            for table in tables:
                create_table_sql = table[0]
                new_cursor.execute(create_table_sql)
    
            new_conn.commit()
            print(f"New database created with schema at {new_db_path}")
    
        except sqlite3.Error as e:
            print(f"SQLite error: {e}")
    
        finally:
            if source_conn:
                source_conn.close()
            if new_conn:
                new_conn.close()

    @staticmethod
    def execute_query(db_path: str, query: str):
        conn = None
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            cursor.execute(query)
            if query.lstrip().upper().startswith("SELECT"):
                results = cursor.fetchall()
                return results
            else:
                conn.commit()
                return None
        except sqlite3.Error as e:
            print(f"An error occurred: {e}")
            return []
        finally:
            if conn is not None:
                conn.close()

    @staticmethod
    def get_schema(db_path: str) -> List[str]:
        conn = None
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT sql FROM sqlite_master WHERE type='table';")
            table_schemas = cursor.fetchall()

            schema_columns = []
            for table_schema in table_schemas:
                create_statement = table_schema[0]
                columns = SQLCommandManager.get_schema_columns(
                    create_statement)
                schema_columns.extend(columns)

            return schema_columns
        except sqlite3.Error as e:
            print(f"An error occurred while retrieving schema: {e}")
            return []
        finally:
            if conn is not None:
                conn.close()

    @staticmethod
    def get_schema_columns(create_statement: str) -> List[str]:
        import re
        columns = re.findall(
            r'\b(\w+)\s+(INTEGER|TEXT|REAL|BLOB|NUMERIC)\b', create_statement)
        return [col[0] for col in columns]

    @staticmethod
    def execute_select(db_path: str, query: str, table_name: str):
        conn = None
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            cursor.execute(
                f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
            if not cursor.fetchone():
                return [], []

            cursor.execute(query)
            results = cursor.fetchall()
            result_columns = [desc[0] for desc in cursor.description]
            return results, result_columns
        except sqlite3.Error as e:
            print(f"Error executing select query on database {db_path}: {e}")
            return [], []
        finally:
            if conn is not None:
                conn.close()

    @staticmethod
    def execute_insert(db_path: str, query: str):
        SQLCommandManager.execute_query(db_path, query)

    @staticmethod
    def create_deleted_records_table(db_path: str):
        create_table_query = """
        CREATE TABLE IF NOT EXISTS deleted_records_in_version (
            _record_id INTEGER NOT NULL,
            snapshot_id TEXT  NOT NULL,
            table_name TEXT NOT NULL,
            PRIMARY KEY (_record_id, snapshot_id)
        );
        """
        SQLCommandManager.execute_query(db_path, create_table_query)

    @staticmethod
    def table_exists(db_path: str, table_name: str) -> bool:
        conn = None
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            cursor.execute(
                f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
            return cursor.fetchone() is not None
        except sqlite3.Error as e:
            print(f"Error checking table existence in database {db_path}: {e}")
            return False
        finally:
            if conn is not None:
                conn.close()

    @staticmethod
    def insert_deleted_record(db_path: str, record_id: int, snapshot_id: str, table_name: str):
        query = f"""
        INSERT INTO deleted_records_in_version (_record_id, snapshot_id, table_name)
        VALUES ({record_id}, '{snapshot_id}', '{table_name}');
        """
        SQLCommandManager.execute_query(db_path, query)

    @staticmethod
    def get_deleted_records(db_path: str, table_name: str) -> List[Tuple]:
        query = f"SELECT * FROM deleted_records_in_version WHERE table_name='{
            table_name}'"
        return SQLCommandManager.execute_query(db_path, query)

    @staticmethod
    def execute_create_table(db_path: str, query: str):
        SQLCommandManager.execute_query(db_path, query)

    @staticmethod
    def create_database(db_path: str):
        if os.path.exists(db_path):
            raise AlreadyExistsError(
                f"Database already exists at path: {db_path}")

        try:
            conn = sqlite3.connect(db_path)
            print(f"333333333333333Database created at path: {db_path}")
            conn.close()
        except sqlite3.Error as e:
            raise DatabaseCreationError(f"Error creating database: {e}")
