"""
DBInstanceService

This class provides the core functionality for managing database instances and their snapshots.
It handles the creation, deletion, modification, and querying of database instances,
as well as managing snapshots and executing SQL queries.

The service interacts directly with the database and file system to perform its operations.

Key Features:
    - Database instance lifecycle management (create, delete, modify, describe)
    - Snapshot creation and management
-Shell support for sql command in dbs in db_instance
   

Methods:
    create: Create a new database instance.
    delete: Delete an existing database instance.
    describe: Get a description of a database instance.
    modify: Modify an existing database instance.
    get: Retrieve a specific database instance.
    create_snapshot: Create a new snapshot of a database instance.
    delete_snapshot: Delete an existing snapshot.
    restore_version: Restore a database instance to a specific version.
    execute_query: Execute a SQL query on a database instance.
    stop: Stop a running database instance.
    start: Start a stopped database instance.
"""

from datetime import datetime
from DB.NEW_KT_DB.DataAccess.DBInstanceManager import DBInstanceManager
from DB.NEW_KT_DB.Exceptions.DBInstanceExceptions import DbSnapshotIdentifierNotFoundError, InvalidQueryError, DatabaseNotFoundError, AlreadyExistsError, DatabaseCreationError
from DB.NEW_KT_DB.Models.DBInstanceModel import DBInstanceModel, Node_SubSnapshot
from DB.NEW_KT_DB.Service.Abc.DBO import DBO
from DB.NEW_KT_DB.Validation.DBInstanceValiditions import validate_allocated_storage, validate_master_user_password, validate_port, validate_status
from collections import deque
from Storage.NEW_KT_Storage.DataAccess.StorageManager import StorageManager
from DB.NEW_KT_DB.DataAccess.SQLCommandManager import SQLCommandManager
import os
import re
from typing import List, Tuple
import uuid
import json


class DBInstanceService(DBO):
    def __init__(self, dal: DBInstanceManager ):
        self.dal = dal
        self.storageManager = StorageManager(DBInstanceModel.BASE_PATH)

    def create(self, **kwargs):
        # Perform validations
        # Validation.validate_db_instance_params(kwargs)
        
        db_instance = DBInstanceModel(**kwargs)
        
        self.storageManager.create_directory(db_instance.db_instance_identifier)

        # Create physical database if db_name is provided
        if 'db_name' in kwargs:
            SQLCommandHelper.create_database(
                kwargs['db_name'], db_instance._last_node_of_current_version, db_instance.endpoint)
      
        # Save to management table
        self.dal.createInMemoryDBInstance(db_instance)

        return db_instance

    def delete(self, db_instance_identifier):
        '''Delete an existing DBInstance.'''
        self.dal.deleteInMemoryDBInstance(db_instance_identifier)
        self.storageManager.delete_directory(db_instance_identifier)

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

        # Validate the provided attributes using specific validation functions
        if 'allocated_storage' in kwargs:
            validate_allocated_storage(kwargs['allocated_storage'])

        if 'master_user_password' in kwargs:
            validate_master_user_password(kwargs['master_user_password'])

        if 'port' in kwargs:
            validate_port(kwargs['port'])

        if 'status' in kwargs:
            validate_status(kwargs['status'])

        # Update attributes after validation
        for attr, value in kwargs.items():
            if attr in modifiable_attributes:
                setattr(db_instance, attr, value)
            else:
                print(f"Warning: Attribute '{attr}' cannot be modified or does not exist.")

        # Apply the changes to the database instance
        self.dal.modifyDBInstance(db_instance)

        return db_instance    

    def get(self, db_instance_identifier):
        db_instance_data = self.dal.getDBInstance(db_instance_identifier)
    
        if not db_instance_data or len(db_instance_data) == 0:
            raise ValueError(f"DB Instance with identifier {db_instance_identifier} not found.")

        instance_json = db_instance_data[0][1]
        instance_dict = json.loads(instance_json)
    
        instance_dict['allocated_storage'] = int(instance_dict['allocated_storage'])
        instance_dict['port'] = int(instance_dict['port'])
        instance_dict['created_time'] = datetime.fromisoformat(instance_dict['created_time'])
    
        instance_dict['node_subSnapshot_dic'] = {uuid.UUID(k): v for k, v in instance_dict['node_subSnapshot_dic'].items()}
        instance_dict['_current_version_ids_queue'] = deque(uuid.UUID(id_str) for id_str in instance_dict['current_version_ids_queue'])
    
        # nodes = {id: None for id in instance_dict['node_subSnapshot_dic']}
        nodes = {id if isinstance(id, uuid.UUID) else uuid.UUID(id): None for id in instance_dict['node_subSnapshot_dic']}

        def revive_node(node_id):
            if nodes[node_id] is not None:
                return nodes[node_id]
    
            node_data = instance_dict['node_subSnapshot_dic'][node_id]
            parent_id = node_data['parent_id']
    
            if parent_id is not None and parent_id in nodes:
                parent = revive_node(uuid.UUID(parent_id))
            else:
                parent = None
    
            node = Node_SubSnapshot(
                parent=parent,
                endpoint=instance_dict['endpoint'],
                id_snapshot=node_data['id_snapshot'],
                dbs_paths_dic=node_data['dbs_paths_dic'],
                deleted_records_db_path=node_data['deleted_records_db_path']
            )

            nodes[node_id] = node
            return node
    
        for node_id in nodes:
            revive_node(node_id)
    
        db_instance = DBInstanceModel(
            db_instance_identifier=instance_dict['db_instance_identifier'],
            allocated_storage=instance_dict['allocated_storage'],
            master_username=instance_dict['master_username'],
            master_user_password=instance_dict['master_user_password'],
            port=instance_dict['port'],
            status=instance_dict['status'],
            created_time=instance_dict['created_time'],
            endpoint=instance_dict['endpoint'],
            _node_subSnapshot_dic=nodes,
            _node_subSnapshot_name_to_id=instance_dict['node_subSnapshot_name_to_id'],
            _current_version_ids_queue=instance_dict['_current_version_ids_queue'],
            pk_column=instance_dict.get('pk_column', 'db_instance_identifier'),
            pk_value=instance_dict.get('pk_value', instance_dict['db_instance_identifier'])
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
        if db_snapshot_identifier not in db_instance._node_subSnapshot_name_to_id:
            raise DbSnapshotIdentifierNotFoundError(f"Snapshot '{db_snapshot_identifier}' not found for instance '{db_instance_identifier}'")

        db_instance._node_subSnapshot_name_to_id.pop(db_snapshot_identifier, None)
        self.dal.modifyDBInstance(db_instance)


    def restore_version(self, db_instance_identifier, db_snapshot_identifier):
        db_instance = self.get(db_instance_identifier)

        if db_snapshot_identifier not in db_instance._node_subSnapshot_name_to_id:
            raise DbSnapshotIdentifierNotFoundError(
                f"Snapshot identifier '{db_snapshot_identifier}' not found.")

        node_id = db_instance._node_subSnapshot_name_to_id[db_snapshot_identifier]
        snapshot_id_uuid = uuid.UUID(node_id)
        snapshot = db_instance._node_subSnapshot_dic.get(snapshot_id_uuid)
        
        if snapshot:
            self._update_queue_to_current_version(snapshot, db_instance)
            self._create_child_to_node(db_instance)


        self.dal.modifyDBInstance(db_instance)

        return db_instance

    def describe_snapshot(self, db_instance_identifier, db_snapshot_identifier):
        db_instance = self.get(db_instance_identifier)
        snapshot_id = db_instance._node_subSnapshot_name_to_id.get(db_snapshot_identifier)
        if snapshot_id:
            snapshot_id_uuid = uuid.UUID(snapshot_id)
            snapshot = db_instance._node_subSnapshot_dic.get(snapshot_id_uuid)

            if snapshot:
                return {
                    "SnapshotIdentifier": db_snapshot_identifier,
                    "DBInstanceIdentifier": db_instance_identifier,
                    "SnapshotCreationTime":  snapshot.created_time if hasattr(snapshot, 'created_time') else None,
                    "SnapshotType": snapshot.snapshot_type, 
                }
        return None
    
    def modify_snapshot(self, db_instance_identifier, db_snapshot_identifier, **kwargs):
        db_instance = self.get(db_instance_identifier)

        if db_snapshot_identifier not in db_instance._node_subSnapshot_name_to_id:
            raise DbSnapshotIdentifierNotFoundError(f"Snapshot identifier '{db_snapshot_identifier}' not found.")

        node_id = db_instance._node_subSnapshot_name_to_id[db_snapshot_identifier]
        snapshot = db_instance._node_subSnapshot_dic.get(node_id)

        if snapshot:
            modifiable_attributes = ['description', 'tags']  # Add other modifiable attributes as needed
            for key, value in kwargs.items():
                if key in modifiable_attributes:
                    if hasattr(snapshot, key):
                        setattr(snapshot, key, value)
                    else:
                        print(f"Warning: Attribute '{key}' does not exist for the snapshot.")
                else:
                    print(f"Warning: Attribute '{key}' cannot be modified for snapshots.")

        self.dal.modifyDBInstance(db_instance)

        return snapshot
    
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
                uuid.UUID(snapshot_to_restore.parent_id))
            height -= 1

        while height < queue_len:
            # db_instance._current_version_ids_queue.popleft()
            db_instance._current_version_ids_queue.pop()
            queue_len -= 1

        while snapshot_to_restore.id_snapshot != str(db_instance._current_version_ids_queue[-1]):
            non_shared_nodes_deque.appendleft(snapshot_to_restore.id_snapshot)
            snapshot_to_restore = db_instance._node_subSnapshot_dic.get(
                uuid.UUID(snapshot_to_restore.parent_id))
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
        if db_instance:
            if query_type == 'SELECT':
                node_queue = [db_instance._node_subSnapshot_dic[id] for id in db_instance._current_version_ids_queue]
                return SQLCommandHelper.select(node_queue, db_name, query, set(db_instance._current_version_ids_queue))

            elif query_type == 'INSERT':
                return SQLCommandHelper.insert(db_instance._last_node_of_current_version, query, db_instance._last_node_of_current_version.dbs_paths_dic[db_name])
            elif query_type == 'CREATE':
                if 'TABLE' in query.upper():
                    return SQLCommandHelper.create_table(query, db_instance._last_node_of_current_version.dbs_paths_dic[db_name])
                elif 'DATABASE' in query.upper():
                    result = SQLCommandHelper.create_database(db_name, db_instance._last_node_of_current_version, db_instance.endpoint)
                    self.dal.modifyDBInstance(db_instance)
                    return result
            elif query_type == 'DELETE':
                node_queue = [db_instance._node_subSnapshot_dic[id] for id in db_instance._current_version_ids_queue]
                return SQLCommandHelper.delete_record(node_queue, query, db_name)
            else:
                raise ValueError(f"Unsupported query type: {query_type}")

class SQLCommandHelper:
    record_id = 0

    @staticmethod
    def clone_database_schema(source_db_path: str, new_db_path: str) -> None:
        SQLCommandManager.clone_database_schema(source_db_path, new_db_path)

    @staticmethod
    def _adjust_results_to_schema(results: List[Tuple], result_columns: List[str], schema_columns: List[str]) -> List[Tuple]:
        adjusted_results = []
        for row in results:
            adjusted_row = []
            for col in schema_columns:
                if col in result_columns:
                    adjusted_row.append(row[result_columns.index(col)])
                else:
                    adjusted_row.append(None)
            adjusted_results.append(tuple(adjusted_row))
        return adjusted_results

    @staticmethod
    def _extract_table_name_from_query(query_type, query: str):
        if query_type == 'DELETE':
            match = re.search(r'DELETE\s+FROM\s+(\w+)', query, re.IGNORECASE)
        elif query_type == 'INSERT':
            match = re.search(r"INSERT\s+INTO\s+(\w+)", query, re.IGNORECASE)
        elif query_type == 'SELECT':
            match = re.search(r"SELECT\s+.*?\s+FROM\s+(\w+)", query, re.IGNORECASE)
        else:
            raise ValueError(f"Unsupported query type: {query_type}")
    
        if match:
            return match.group(1)
        else:
            raise InvalidQueryError(f"Failed to extract table name from the '{query_type}' query.")

    @staticmethod
    def select(queue, db_id: str, query: str, snapshots_ids_in_current_version_set: set):
        all_results = []
        current_node = queue[-1]
        current_db_path = current_node.dbs_paths_dic.get(db_id)

        if not current_db_path:
            raise ValueError(f"Database '{db_id}' not found in the first node.")

        schema_columns = SQLCommandManager.get_schema(current_db_path)

        table_name = SQLCommandHelper._extract_table_name_from_query('SELECT', query)

        deleted_records = SQLCommandHelper._union_deleted_records(queue, table_name)

        deleted_records_map = {}
        for _record_id, snapshot_id, _ in deleted_records:
            if _record_id not in deleted_records_map:
                deleted_records_map[_record_id] = set()
            deleted_records_map[_record_id].add(uuid.UUID(snapshot_id))

        for node in queue:
            db_path = node.dbs_paths_dic.get(db_id)
            if db_path:
                results, result_columns = SQLCommandManager.execute_select(db_path, query, table_name)
                filtered_results = []
                for row in results:
                    record_id = row[0]
                    if record_id in deleted_records_map:
                        deleted_snapshots = deleted_records_map[record_id]
                        if deleted_snapshots.intersection(snapshots_ids_in_current_version_set):
                            continue
                    filtered_results.append(row)

                adjusted_results = SQLCommandHelper._adjust_results_to_schema(filtered_results, result_columns, schema_columns)
                all_results.extend(adjusted_results)

        return all_results

    @staticmethod
    def insert(last_node_of_current_version, query: str, db_path):
        table_name = SQLCommandHelper._extract_table_name_from_query("INSERT", query)

        if db_path not in last_node_of_current_version.dbs_paths_dic.values():
            raise DatabaseNotFoundError(f"Database path: '{db_path}' for table '{table_name}' not found.")

        if "VALUES" in query:
            columns_section = query[query.index('(') + 1:query.index(')')]
            values_section = query[query.index('VALUES') + 6:].strip().rstrip(';')

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

            new_values_list = []
            for value in values_list:
                new_values = f"{SQLCommandHelper.record_id}, {value}"
                new_values_list.append(new_values)
                SQLCommandHelper.record_id += 1

            new_values_section = "),(".join(new_values_list)
            query = f"INSERT INTO {table_name} (_record_id, {columns_section}) VALUES ({new_values_section});"

        else:
            raise ValueError("Unsupported INSERT query format.")

        SQLCommandManager.execute_insert(db_path, query)

    @staticmethod
    def delete_record(queue, delete_query, db_name):
        try:
            table_name = SQLCommandHelper._extract_table_name_from_query('DELETE', delete_query)
            current_node = queue[-1]

            SQLCommandManager.create_deleted_records_table(current_node.deleted_records_db_path)

            for node in queue:
                db_path = node.dbs_paths_dic.get(db_name)
                if db_path:
                    if SQLCommandManager.table_exists(db_path, table_name):
                        find_records_query = delete_query.replace("DELETE", f"SELECT _record_id")
                        records_to_delete = SQLCommandManager.execute_select(db_path, find_records_query, table_name)[0]
                        for record_id in records_to_delete:
                            SQLCommandManager.insert_deleted_record(current_node.deleted_records_db_path, record_id[0], current_node.id_snapshot, table_name)
                        print(f"Records from {table_name} marked as deleted in DB: {db_name} at snapshot: {current_node.id_snapshot}")
                        return
                    else:
                        print(f"Table {table_name} not found in DB: {db_name}. Continuing to next DB...")

            print("No matching record found to delete.")

        except InvalidQueryError as e:
            print(f"Invalid query: {e}")

    @staticmethod
    def _union_deleted_records(nodes_queue, table_name):
        union_results = []
        for node in nodes_queue:
            records = SQLCommandManager.get_deleted_records(node.deleted_records_db_path, table_name)
            union_results.extend(records)
        return union_results

    @staticmethod
    def create_table(query: str, db_path):
        fields_start = query.upper().find('(') + 1
        fields_end = query.rfind(')')
        fields = query[fields_start:fields_end].strip()
        new_fields = f"_record_id INTEGER, {fields}"
        new_query = query[:fields_start] + new_fields + query[fields_end:]
        SQLCommandManager.execute_create_table(db_path, new_query)

    @staticmethod
    def create_database(db_name, last_node_of_current_version, endpoint):
        db_filename = f"{db_name}.db"
        db_path = os.path.join(endpoint, str(last_node_of_current_version.id_snapshot), db_filename)
        SQLCommandManager.create_database(db_path)
        last_node_of_current_version.dbs_paths_dic[db_name] = db_path
