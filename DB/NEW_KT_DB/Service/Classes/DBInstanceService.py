import datetime
from DataAccess import DBInstanceManager
from Models import DBInstanceModel
from Abc import DBO
from Validation import Validation
from collections import deque
import os
import sqlite3
import re
from typing import Deque, List, Tuple
from DB.NEW_KT_DB.Models.DBInstanceModel import Node_SubSnapshot

class DBInstanceService(DBO):
    def __init__(self, dal: DBInstanceManager):
        self.dal = dal

    def create(self, **kwargs):
        # Perform validations
        Validation.validate_db_instance_params(kwargs)
        # Create DBInstance model
        db_instance = DBInstanceModel(**kwargs)
        # Save to management table
        self.dal.createInMemoryDBInstance(db_instance)
        # Create physical database if db_name is provided
        if 'db_name' in kwargs:
            SQLCommandHelper.create_database(
                kwargs['db_name'], db_instance._last_node_of_current_version, db_instance.endpoint)

        return db_instance

    def delete(self, db_instance_identifier):
        '''Delete an existing DBInstance.'''
        # Delete from memory using DBInstanceManager.deleteInMemoryDBInstance() function
        self.dal.deleteInMemoryDBInstance(db_instance_identifier)

    def describe(self, db_instance_identifier):
        '''Describe the details of DBInstance.'''
        return self.dal.describeDBInstance(db_instance_identifier)

   
    def modify(self, db_instance_identifier, **kwargs):
        """Modify an existing DBInstance."""
        db_instance = self.get(db_instance_identifier)

        modifiable_attributes = [
            'allocated_storage',
            'master_user_password',
            'port',
            'status'
        ]

        for attr, value in kwargs.items():
            if attr in modifiable_attributes:
                setattr(db_instance, attr, value)
            else:
                print(f"Warning: Attribute '{attr}' cannot be modified or does not exist.")

        self.dal.modifyDBInstance(db_instance)

        return db_instance    


    def get(self, db_instance_identifier):
        db_instance_data = self.dal.getDBInstance(db_instance_identifier)

        db_instance_data['allocated_storage'] = int(db_instance_data['allocated_storage'])
        db_instance_data['port'] = int(db_instance_data['port'])
        db_instance_data['created_time'] = datetime.fromisoformat(db_instance_data['created_time'])

        db_instance_data['node_subSnapshot_dic'] = {uuid.UUID(k): v for k, v in db_instance_data['node_subSnapshot_dic'].items()}
        db_instance_data['_current_version_ids_queue'] = deque(uuid.UUID(id_str) for id_str in db_instance_data['current_version_ids_queue'])

        nodes = {id: None for id in db_instance_data['node_subSnapshot_dic']}

        def revive_node(node_id):
            if nodes[node_id] is not None:
                return nodes[node_id]

            node_data = db_instance_data['node_subSnapshot_dic'][node_id]
            parent_id = node_data['parent_id']

            if parent_id is not None and parent_id in nodes:
                parent = revive_node(parent_id)
            else:
                parent = None

            node = Node_SubSnapshot(
                parent=parent,
                endpoint=db_instance_data['endpoint'],
                id_snapshot=node_data['id_snapshot'],
                dbs_paths_dic=node_data['dbs_paths_dic'],
                deleted_records_db_path=node_data['deleted_records_db_path']
            )

            nodes[node_id] = node
            return node
 
        for node_id in nodes:
            revive_node(node_id)

        db_instance = DBInstanceModel(
            db_instance_identifier=db_instance_data['db_instance_identifier'],
            allocated_storage=db_instance_data['allocated_storage'],
            master_username=db_instance_data['master_username'],
            master_user_password=db_instance_data['master_user_password'],
            port=db_instance_data['port'],
            status=db_instance_data['status'],
            created_time=datetime.fromisoformat(db_instance_data['created_time']),
            endpoint=db_instance_data['endpoint'],
            _node_subSnapshot_dic=nodes,
            _node_subSnapshot_name_to_id=db_instance_data['node_subSnapshot_name_to_id'],
            _current_version_ids_queue=deque(
                db_instance_data['current_version_ids_queue'])
        )

        return db_instance

    def create_snapshot(self, db_instance_identifier, db_snapshot_identifier):
        db_instance = self.get(db_instance_identifier)
        setattr(db_instance,'created_time', datetime.now())
        db_instance._node_subSnapshot_name_to_id[db_snapshot_identifier] = db_instance._last_node_of_current_version.id_snapshot
        self._create_child_to_node(db_instance)
        self.dal.modifyDBInstance(db_instance)

    def delete_snapshot(self, db_instance_identifier, db_snapshot_identifier):
        db_instance = self.get(db_instance_identifier)
        db_instance._node_subSnapshot_name_to_id.pop(
            db_snapshot_identifier, None)
        self.dal.modifyDBInstance(db_instance)

    def restore_version(self, db_instance_identifier, db_snapshot_identifier):
        db_instance = self.get(db_instance_identifier)

        if db_snapshot_identifier not in db_instance._node_subSnapshot_name_to_id:
            raise DbSnapshotIdentifierNotFoundError(
                f"Snapshot identifier '{db_snapshot_identifier}' not found.")

        node_id = db_instance._node_subSnapshot_name_to_id[db_snapshot_identifier]
        snapshot = db_instance._node_subSnapshot_dic.get(node_id)

        if snapshot:
            self._update_queue_to_current_version(snapshot, db_instance)
            self._create_child_to_node(db_instance)

        self.dal.modifyDBInstance(db_instance)

        return db_instance

    def stop(self, db_instance_identifier):
        db_instance = self.get(db_instance_identifier)
        db_instance.status = 'stopped'
        self.dal.modifyDBInstance(db_instance)

    def start(self, db_instance_identifier):
        db_instance = self.get(db_instance_identifier)
        db_instance.status = 'available'
        self.dal.modifyDBInstance(db_instance)

    def __get_node_height(self, current_node, node_subSnapshot_dic):
        height = 0
        while current_node:
            height += 1
            current_node = node_subSnapshot_dic.get(current_node.parent_id)
        return height

    def _update_queue_to_current_version(self, snapshot_to_restore, db_instance):
        height = self.__get_node_height(
            snapshot_to_restore, db_instance._node_subSnapshot_dic)
        non_shared_nodes_deque = deque()
        queue_len = len(db_instance._current_version_ids_queue)

        while height > queue_len:
            non_shared_nodes_deque.appendleft(snapshot_to_restore.id_snapshot)
            snapshot_to_restore = db_instance._node_subSnapshot_dic.get(
                snapshot_to_restore.parent_id)
            height -= 1

        while height < queue_len:
            db_instance._current_version_ids_queue.popleft()
            queue_len -= 1

        while snapshot_to_restore.id_snapshot != db_instance._current_version_ids_queue[-1]:
            non_shared_nodes_deque.appendleft(snapshot_to_restore.id_snapshot)
            snapshot_to_restore = db_instance._node_subSnapshot_dic.get(
                snapshot_to_restore.parent_id)
            db_instance._current_version_ids_queue.popleft()

        db_instance._current_version_ids_queue.extend(non_shared_nodes_deque)

    def _create_child_to_node(self, db_instance):
        node = db_instance._last_node_of_current_version
        db_instance._last_node_of_current_version = node.create_child(
            db_instance.endpoint)
        db_instance._current_version_ids_queue.append(
            db_instance._last_node_of_current_version.id_snapshot)
        db_instance._node_subSnapshot_dic[db_instance._last_node_of_current_version.id_snapshot] = db_instance._last_node_of_current_version

    def execute_query(self, db_instance_identifier, query, db_name):
        db_instance = self.get(db_instance_identifier)
        query_type = query.strip().split()[0].upper()

        if query_type == 'SELECT':
            return SQLCommandHelper.select(db_instance._current_version_ids_queue, db_name, query, set(db_instance._current_version_ids_queue))
        elif query_type == 'INSERT':
            return SQLCommandHelper.insert(db_instance._last_node_of_current_version, query, db_name)
        elif query_type == 'CREATE':
            if 'TABLE' in query.upper():
                return SQLCommandHelper.create_table(query, db_instance._last_node_of_current_version.dbs_paths_dic[db_name])
        elif query_type == 'DELETE':
            return SQLCommandHelper.delete_record(db_instance._current_version_ids_queue, query, db_name)
        else:
            raise ValueError(f"Unsupported query type: {query_type}")


class SQLCommandHelper:

    record_id = 0

    @staticmethod
    def clone_database_schema(source_db_path: str, new_db_path: str) -> None:
        """Clones the schema from an existing SQLite database to a new SQLite database. 

        Args:
            source_db_path: Path to the existing SQLite database file.
            new_db_path: Path where the new SQLite database file should be created.
        """
        source_conn = None
        new_conn = None
        try:
            # Connect to the existing database to get schema
            source_conn = sqlite3.connect(source_db_path)
            source_cursor = source_conn.cursor()

            # Connect to the new database
            new_conn = sqlite3.connect(new_db_path)
            new_cursor = new_conn.cursor()

            # Get the schema for tables from the existing database
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
    def _run_query(db_path: str, query: str):
        """
        Executes a SQL query on the specified database. 

        Args:
            db_path: Path to the SQLite database file.
            query: SQL query to be executed.    

        Returns:
            A list of tuples containing query results if the query retrieves data,
            or None if the query doesn't return results.
        """
        conn = None  # Initialize conn to None

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
                conn.close()  # Close the connection if it was successfully opened

    @staticmethod
    def _get_schema_columns(create_statement: str) -> List[str]:
        """Extracts column names from a CREATE TABLE statement. 

        Args:
            create_statement: SQL CREATE TABLE statement.   

        Returns:
            A list of column names.
        """
        columns = re.findall(
            r'\b(\w+)\s+(INTEGER|TEXT|REAL|BLOB|NUMERIC)\b', create_statement)
        return [col[0] for col in columns]

    @staticmethod
    def _get_schema(db_path: str) -> List[str]:
        """Retrieves the schema of all tables in the database.  

        Args:
            db_path: Path to the SQLite database file.  

        Returns:
            A list of column names for all tables in the database.
        """
        conn = None  # Initialize conn to None

        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT sql FROM sqlite_master WHERE type='table';")
            table_schemas = cursor.fetchall()

            schema_columns = []
            for table_schema in table_schemas:
                create_statement = table_schema[0]
                columns = SQLCommandHelper._get_schema_columns(
                    create_statement)
                schema_columns.extend(columns)

            return schema_columns

        except sqlite3.Error as e:
            print(f"An error occurred while retrieving schema: {e}")
            return []

        finally:
            if conn is not None:
                conn.close()  # Close the connection if it was successfully opened

    @staticmethod
    def _adjust_results_to_schema(results: List[Tuple], result_columns: List[str], schema_columns: List[str]) -> List[Tuple]:
        """Adjusts query results to match the schema of the first database. 

        Args:
            results: List of query results.
            result_columns: List of column names from the query results.
            schema_columns: List of column names from the first database schema.    

        Returns:
            A list of tuples containing adjusted results.
        """
        adjusted_results = []
        for row in results:
            adjusted_row = []
            for col in schema_columns:
                if col in result_columns:
                    adjusted_row.append(row[result_columns.index(col)])
                else:
                    # Default value for missing columns
                    adjusted_row.append(None)
            adjusted_results.append(tuple(adjusted_row))
        return adjusted_results

    @staticmethod
    def _extract_table_name_from_query(query_type, query: str):
        """
        Extracts the table name from an SQL query based on the specified query type.    

        Parameters:
            query_type (Literal['INSERT', 'DELETE']): The type of SQL query, either 'INSERT' or 'DELETE'.
            query (str): The SQL query string.  

        Returns:
            str: The extracted table name.  

        Raises:
            InvalidQueryError: If the table name cannot be extracted from the query.
        """
        if query_type == 'DELETE':
            match = re.search(r'DELETE\s+FROM\s+(\w+)', query, re.IGNORECASE)
        elif query_type == 'INSERT':
            match = re.search(r"INSERT\s+INTO\s+(\w+)", query, re.IGNORECASE)

        if match:
            return match.group(1)
        else:
            raise InvalidQueryError(f"Failed to extract table name from the '{query_type}' query.")

    @staticmethod
    def _union_deleted_records(nodes_queue, table_name):
        """Unions deleted records from multiple databases based on a specified table name.  

        Args:
            nodes_queue: A deque containing nodes with database paths.
            table_name: Name of the table from which to union deleted records.  

        Returns:
            A list of deleted records from the specified table.
        """
        union_results = []

        for node in nodes_queue:
            db_path = node.deleted_records_db_path
            print("union_deleted_records: ", db_path)
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()

            try:
                # Check if the table exists
                cursor.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='deleted_records_in_version'")
                if cursor.fetchone():
                    # If the table exists, perform a SELECT query
                    cursor.execute(
                        "SELECT * FROM deleted_records_in_version WHERE table_name=?", (table_name,))
                    records = cursor.fetchall()
                    # Add records to the main list
                    union_results.extend(records)
            except sqlite3.Error as e:
                print(f"Error accessing database {db_path}: {e}")
            finally:
                conn.close()

        return union_results

    @staticmethod
    def select(queue, db_id: str, query: str, snapshots_ids_in_current_version_set: set):
        """
        Executes the given query across all databases in the queue and returns the results adjusted to match
        the schema of the first database.   

        Args:
            queue (List[object]): List of nodes where each node contains a dictionary of database paths.
            db_id (str): Identifier for the specific database.
            query (str): SQL query to be executed.
            snapshots_ids_in_current_version_set (set): Set of snapshot IDs relevant to the current version.    

        Returns:
            List[Tuple]: List of tuples containing query results adjusted to the schema of the first database.
        """
        all_results = []
        current_node = queue[-1]
        current_db_path = current_node.dbs_paths_dic.get(db_id)

        if not current_db_path:
            raise ValueError(
                f"Database '{db_id}' not found in the first node.")

        # Extract the schema of the first database
        schema_columns = SQLCommandHelper._get_schema(current_db_path)

        # Extract the table name from the original query
        table_name_match = re.search(r'FROM\s+(\w+)', query, re.IGNORECASE)
        if not table_name_match:
            raise ValueError("Unable to extract table name from query.")

        table_name = table_name_match.group(1)

        # Union the deleted records from the queues
        deleted_records = SQLCommandHelper._union_deleted_records(
            queue, table_name)
        deleted_records_map = {}
        print("deleted_records :", deleted_records)
        # Build a mapping by record_id
        for _record_id, snapshot_id, _ in deleted_records:
            if _record_id not in deleted_records_map:
                deleted_records_map[_record_id] = set()
            deleted_records_map[_record_id].add(snapshot_id)

        print("deleted_records_map: ", deleted_records_map)

        for node in queue:
            db_path = node.dbs_paths_dic.get(db_id)
            if db_path:
                try:
                    with sqlite3.connect(db_path) as conn:
                        cursor = conn.cursor()

                        # Execute the original query
                        cursor.execute(query)
                        results = cursor.fetchall()
                        print("results: ", results)

                        result_columns = [desc[0]
                                          for desc in cursor.description]

                        # Filter results
                        filtered_results = []
                        for row in results:
                            record_id = row[0]
                            # Check for deleted records for the current record_id
                            if record_id in deleted_records_map:
                                deleted_snapshots = deleted_records_map[record_id]

                                # Check if any of the deleted snapshot IDs are in the current version set
                                if deleted_snapshots.intersection(snapshots_ids_in_current_version_set):
                                    continue  # Skip this record as it is deleted

                            filtered_results.append(row)

                        # Adjust the results to match the schema of the first database
                        adjusted_results = SQLCommandHelper._adjust_results_to_schema(
                            filtered_results, result_columns, schema_columns)
                        all_results.extend(adjusted_results)

                except sqlite3.Error as e:
                    print(f"Error executing query on database {db_path}: {e}")

        return all_results

    @staticmethod
    def create_table(query: str, db_path):
        """
        Modifies the table creation query to include a '_record_id' column as the first column
        and then executes it using the _run_query function. 

        Args:
            query: The SQL query to create a table.
            db_path: The path to the SQLite database file.
        """
        # Finding the position of the fields in the SQL query
        fields_start = query.upper().find('(') + 1
        fields_end = query.rfind(')')

        # Creating the fields string with the addition of _record_id at the beginning
        fields = query[fields_start:fields_end].strip()
        new_fields = f"_record_id INTEGER, {fields}"
        new_query = query[:fields_start] + new_fields + query[fields_end:]
        SQLCommandHelper._run_query(db_path, new_query)

    @staticmethod
    def insert(last_node_of_current_version, query: str, db_path):
        """
        Executes an INSERT query on the database associated with the last node of the current version,
        including an insertion of the global record_id into the _record_id column where applicable. 

        Parameters:
            last_node_of_current_version (Node_SubSnapshot): The last node in the current version chain containing the DB paths.
            query (str): The SQL INSERT query to be executed.   

        Raises:
            DatabaseNotFoundError: If the database path corresponding to the table name is not found.
            ConnectionError: If the INSERT operation fails or returns no results.
        """
        global record_id

        table_name = SQLCommandHelper._extract_table_name_from_query(
            "INSERT", query)

        if db_path not in last_node_of_current_version.dbs_paths_dic.values():
            raise DatabaseNotFoundError(f"Database path: '{db_path}' for table '{
                                        table_name}' not found.")

        if "VALUES" in query:
            # Extracting the columns section
            columns_section = query[query.index('(') + 1:query.index(')')]

            # Extracting and cleaning the values section
            values_section = query[query.index(
                'VALUES') + 6:].strip().rstrip(';')

            # Splitting the values into separate items manually
            values_list = []
            current_value = ""
            inside_value = False

            for char in values_section:
                if char == '(':
                    inside_value = True
                    if current_value:
                        current_value += char
                    else:
                        current_value = char
                elif char == ')':
                    current_value += char
                    inside_value = False
                    values_list.append(current_value.strip("()"))
                    current_value = ""
                elif inside_value:
                    current_value += char

            # Create new values with _record_id
            new_values_list = []
            for value in values_list:
                new_values = f"{record_id}, {value}"
                new_values_list.append(new_values)
                record_id += 1

            # Constructing the new query with _record_id column
            new_values_section = "),(".join(new_values_list)
            query = f"INSERT INTO {table_name} (_record_id, {columns_section}) VALUES ({
                new_values_section});"

        else:
            raise ValueError("Unsupported INSERT query format.")

        # For debugging, this should now print the correct query
        print("query: ", query)
        SQLCommandHelper._run_query(db_path, query)

    @staticmethod
    def delete_record(queue, delete_query, db_name):
        """
        Moves deleted records to a 'deleted_record' table instead of updating a 'deleted' column.   

        Parameters:
            queue (Deque['Node_SubSnapshot']): A deque containing nodes with database paths.
            delete_query (str): The original SQL DELETE query to be modified and executed.
            current_id_snapshot (int): The current snapshot ID to mark the deleted records. 

        Raises:
            InvalidQueryError: If the query execution fails or the query is not constructed properly.
        """
        try:

            table_name = SQLCommandHelper._extract_table_name_from_query(
                'DELETE', delete_query)
            current_node = queue[-1]

            # Create the 'deleted_records_in_version' table if it doesn't exist
            create_deleted_table_query = f"""
            CREATE TABLE IF NOT EXISTS deleted_records_in_version (
                _record_id INTEGER NOT NULL,
                snapshot_id INTEGER NOT NULL,
                table_name TEXT NOT NULL,
                PRIMARY KEY (_record_id, snapshot_id)
            );
            """
            SQLCommandHelper._run_query(current_node.deleted_records_db_path,
                                        create_deleted_table_query)

            for node in queue:

                # for db_name, db_path in node.dbs_paths_dic.items():
                try:
                    db_path = node.dbs_paths_dic.get(db_name)
                    # Check if the table exists in the current database
                    if db_path:
                        check_table_query = f"SELECT name FROM sqlite_master WHERE type='table' AND name='{
                            table_name}';"
                        result = SQLCommandHelper._run_query(
                            db_path, check_table_query)
                        if result:  # If the table exists, execute the modified query

                            # Find the records to delete and insert them into 'deleted_records_in_version'
                            find_records_query = delete_query.replace(
                                "DELETE", f"SELECT _record_id")
                            print("find_records_query", find_records_query)
                            records_to_delete = SQLCommandHelper._run_query(
                                db_path, find_records_query)
                            for record_id in records_to_delete:
                                insert_deleted_query = f"""
                                INSERT INTO deleted_records_in_version (_record_id, snapshot_id, table_name)
                                VALUES ({record_id[0]}, {
                                    current_node.id_snapshot}, '{table_name}');
                                """
                                SQLCommandHelper._run_query(
                                    current_node.deleted_records_db_path, insert_deleted_query)
                            print(f"Records from {table_name} marked as deleted in DB: {db_name} at snapshot: {current_node.id_snapshot}")
                            return
                        else:
                            print(f"Table {table_name} not found in DB: {db_name}. Continuing to next DB...")
                except sqlite3.Error as e:
                    print(f"Error while accessing database {db_name}: {e}")
                    raise InvalidQueryError(
                        f"The query: {e} - failed to execute or was not constructed properly.")

            print("No matching record found to delete.")

        except InvalidQueryError as e:
            print(f"Invalid query: {e}")

    @staticmethod
    def create_database(db_name, last_node_of_current_version, endpoint):
        """
        Creates a SQLite database file at the given path.   

        Args:
            db_path: The path where the database file will be created.  

        Raises:
            AlreadyExistsError: If the database already exists at the given path.
            DatabaseCreationError: If there is an error creating the database.
        """
        conn = None  # Initialize the connection variable
        db_filename = f"{db_name}.db"
        db_path = os.path.join(
            endpoint, str(last_node_of_current_version.id_snapshot))
        os.makedirs(db_path, exist_ok=True)
        db_path = os.path.join(db_path, db_filename)
        print(db_path)

        try:
            if os.path.exists(db_path):
                raise AlreadyExistsError(
                    f"Database already exists at path: {db_path}")

            # Create a connection to the database (creates the file if it doesn't exist)
            conn = sqlite3.connect(db_path)
            last_node_of_current_version.dbs_paths_dic[db_name] = db_path
            print(f"Database created successfully at path: {db_path}")
        except AlreadyExistsError as e:
            raise  # Reraise the already exists error for higher-level handling
        except sqlite3.Error as e:
            raise DatabaseCreationError(f"Error creating database: {e}")
        except Exception as e:
            raise DatabaseCreationError(f"An unexpected error occurred: {e}")
        finally:
            # Close the connection to the database if it was opened
            if conn:
                conn.close()


class DbSnapshotIdentifierNotFoundError(Exception):
    """Raised when a db snapshot identifier is not found."""
    pass


class AlreadyExistsError(Exception):
    """Raised when an object already exists."""
    pass


class DatabaseCreationError(Exception):
    """Raised when there is an error creating the database."""
    pass


class InvalidQueryError(Exception):
    """Raised when a query is not properly constructed or contains syntax errors."""
    pass


class DatabaseCloneError(Exception):
    """Custom exception for database cloning errors."""
    pass


class DatabaseCloneError(Exception):
    """Custom exception for database cloning errors."""
    pass
