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

import copy
from datetime import datetime
import shutil
from KT_Cloud.DB.NEW_KT_DB.DataAccess.DBInstanceReplicaManager import DBInstanceManager
from DB.NEW_KT_DB.Exceptions.DBInstanceExceptions import DbSnapshotIdentifierNotFoundError, InvalidQueryError, DatabaseNotFoundError, AlreadyExistsError, DatabaseCreationError
from KT_Cloud.DB.NEW_KT_DB.Models.DBInstanceReplicaModel import DBInstanceModel, Node_SubSnapshot
from DB.NEW_KT_DB.Service.Abc.DBO import DBO
from KT_Cloud.DB.NEW_KT_DB.Validation.DBInstanceReplicaValiditions import validate_allocated_storage, validate_master_user_password, validate_port, validate_status,validate_master_user_name
from collections import deque
from Storage.NEW_KT_Storage.DataAccess.StorageManager import StorageManager
from DB.NEW_KT_DB.DataAccess.SQLCommandManager import SQLCommandManager
from DB.NEW_KT_DB.Validation.DBInstanceNaiveValidition import check_extra_params, check_required_params, is_valid_db_instance_identifier,MissingRequireParamError
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
        if not kwargs.get('is_replice',False):
            required_params = ['db_instance_identifier', 'master_username', 'master_user_password']
            all_params = ['is_replica','endpoint','status','db_name', 'port', 'allocated_storage','db_cluster_identifier','availability_zone']
            all_params.extend(required_params)

            # Validate required and extra parameters
            check_required_params(required_params, kwargs)
            check_extra_params(all_params, kwargs)

        if kwargs.get('db_cluster_identifier',None) is None:
            from DB.NEW_KT_DB.Controller.DBClusterController import DBClusterController,DBClusterService,DBClusterManager,DBInstanceController
            db_cluster_controller=DBClusterController(DBClusterService(DBClusterManager(self.dal.object_manager),self.storageManager,DBInstanceModel.BASE_PATH),DBInstanceController(self))
            db_cluster_identifier=str(uuid.uuid4())
            db_cluster_controller.create_db_cluster(db_cluster_identifier='a'+(db_cluster_identifier) ,engine= 'mysql',allocated_storage=5,db_subnet_group_name= 'my-subnet-group')
            kwargs['db_cluster_identifier']=db_cluster_identifier
        
        db_instance_identifier=kwargs.get('db_instance_identifier')
        if not is_valid_db_instance_identifier(db_instance_identifier, 63):
            raise ValueError('db_instance_identifier is invalid')
        
        if self.dal.is_db_instance_exist(db_instance_identifier):
            raise AlreadyExistsError('DBinstance',db_instance_identifier)
        
        self._validate_parameters(kwargs=kwargs)
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
            availability_zone=instance_dict['availability_zone'],
            is_replica=instance_dict['is_replica'],
            db_cluster_identifier=instance_dict['db_cluster_identifier'],
            _node_subSnapshot_name_to_id=instance_dict['node_subSnapshot_name_to_id'],
            _current_version_ids_queue=instance_dict['_current_version_ids_queue'],
            pk_column=instance_dict.get('pk_column', 'db_instance_identifier'),
            pk_value=instance_dict.get('pk_value', instance_dict['db_instance_identifier'])
        )
    
        return db_instance

    def create_raed_replica(self,**kwargs):
        """
        Creates a read replica of a DB instance.
        
        Parameters:
        - `source_db_instance_identifier` (str): The identifier of the source DB instance to create the read replica from.
        - `db_instance_identifier` (str): The identifier of the new read replica DB instance.
        - `master_username` (str): The master username for the new read replica DB instance.
        - `master_user_password` (str): The master user password for the new read replica DB instance.
        - `port` (int, optional): The port number for the new read replica DB instance.
        - `allocated_storage` (int, optional): The allocated storage for the new read replica DB instance.
        - `db_cluster_identifier` (str, optional): The DB cluster identifier for the new read replica DB instance.
        - `availability_zone` (str, optional): The availability zone for the new read replica DB instance.
        
        Returns:
        - `read_replica` (DBInstanceModel): The newly created read replica DB instance.
        """
        if 'source_db_instance_identifier' not in kwargs:
            raise MissingRequireParamError('missing the param source_db_instance_identifier')
        required_params = ['source_db_instance_identifier','db_instance_identifier', 'master_username', 'master_user_password']
        all_params = ['port', 'allocated_storage','db_cluster_identifier','availability_zone']
        all_params.extend(required_params)
        
        # Validate required and extra parameters
        check_required_params(required_params, kwargs)
        check_extra_params(all_params, kwargs)
        source_db_instance=self.get(kwargs['source_db_instance_identifier'])
        source_db_instance=self.create_snapshot(source_db_instance.db_instance_identifier, db_snapshot_identifier=f'{source_db_instance.db_instance_identifier}_snapshot_{datetime.now().strftime("%Y%m%d%H%M%S")}',from_create=True)
        read_replica_dict=source_db_instance.to_dict().copy()
        for key, value in kwargs.items():
            if key in kwargs:
                read_replica_dict[key] = value
        read_replica_dict['is_replica']=True
        read_replica_dict.pop('source_db_instance_identifier')
        read_replica_dict.pop('node_subSnapshot_dic')
        read_replica_dict.pop('node_subSnapshot_name_to_id')
        read_replica_dict.pop('current_version_ids_queue')
        read_replica_dict['db_cluster_identifier']=source_db_instance.db_cluster_identifier
        read_replica_dict.pop('created_time')
        read_replica=self.create(**read_replica_dict)
        read_replica._node_subSnapshot_dic=self._copy_dict_with_sqlite(source_db_instance._node_subSnapshot_dic,read_replica.endpoint)
        read_replica._node_subSnapshot_name_to_id=copy.deepcopy(source_db_instance._node_subSnapshot_name_to_id)
        read_replica._current_version_ids_queue=deque(id_str for id_str in source_db_instance._current_version_ids_queue)
        read_replica._last_node_of_current_version=source_db_instance._node_subSnapshot_dic[source_db_instance._current_version_ids_queue[-1]]
        read_replica.is_replica=True
        self.dal.modifyDBInstance(read_replica)

        return read_replica

    def _copy_dict_with_sqlite(self,dict_obj,target_directory):
        """
        Copies a dictionary of Snapshot objects and their associated database files to a new directory.
        
        Parameters:
        - `dict_obj` (dict): The dictionary of Snapshot objects to be copied.
        - `target_directory` (str): The directory path where the copied Snapshot objects and database files will be stored.
        
        Returns:
        - `copied_dict` (dict): A new dictionary containing the copied Snapshot objects with the new file paths.
        """
        copied_dict = {}
        for key, snapshot in dict_obj.items():
            new_snapshot=self._copy_snapshot(snapshot,target_directory)
            copied_dict[key] = new_snapshot

        return copied_dict
    
    def _copy_snapshot(self,snapshot,target_directory):
        """
        Copies a snapshot object and its associated database files to a new directory.
        
        Parameters:
        - `snapshot` (Snapshot): The snapshot object to be copied.
        - `target_directory` (str): The directory path where the copied snapshot and database files will be stored.
        
        Returns:
        - `new_snapshot` (Snapshot): The copied snapshot object with the new file paths.
        """
        new_snapshot = copy.deepcopy(snapshot)
        if not os.path.exists(os.path.join(target_directory, str(new_snapshot.id_snapshot))):
            os.makedirs(os.path.join(target_directory, str(new_snapshot.id_snapshot)))

        new_snapshot_dbs_paths_dic = {}
        for db_name, db_path in snapshot.dbs_paths_dic.items():
            db_filename = os.path.basename(db_path)
            new_db_path = os.path.join(target_directory, str(new_snapshot.id_snapshot), db_filename)
            shutil.copy(db_path, new_db_path)
            new_snapshot_dbs_paths_dic[db_name] = new_db_path
        new_snapshot.dbs_paths_dic = new_snapshot_dbs_paths_dic

        if snapshot.deleted_records_db_path:
            new_deleted_db_path = os.path.join(target_directory, str(new_snapshot.id_snapshot), "deleted_db.db")
            if os.path.exists(snapshot.deleted_records_db_path):
                shutil.copy(snapshot.deleted_records_db_path, new_deleted_db_path)
            new_snapshot.deleted_records_db_path = new_deleted_db_path
        return new_snapshot


    def switchover_read_replica(self, db_instance_identifier):
        '''
        Switches over a primary DB instance to a read replica.
        
        Parameters:
        - `db_instance_identifier` (str): The identifier of the DB instance to switch over.
        
        Returns:
        - `db_instance` (DBInstance): The switched over DB instance.
        
        Raises:
        - `Exception`: If the instance is a read replica or if its status is not available, or if the last node of the current version has pending changes.
        '''
        db_instance = self.get(db_instance_identifier)
        if db_instance.is_replica:
            raise Exception("Switchover is not supported for read replicas.")
        if db_instance.status != 'available':
            raise Exception(f"DB instance {db_instance_identifier} is not available.")
        if db_instance._last_node_of_current_version.has_pending_changes():
            self.create_snapshot(db_instance_identifier, db_snapshot_identifier=f'{db_instance_identifier}_snapshot_{datetime.now().strftime("%Y%m%d%H%M%S")}')
        self.add_replica_to_cluster(db_instance)
        setattr(db_instance,'is_replica',True)
        self.dal.modifyDBInstance(db_instance)
        return db_instance
        
    def failover_db_instance(self, db_instance_identifier):
        '''
        Promotes a read replica to become the primary instance in this cluster.
        
        Parameters:
        - `db_instance_identifier` (str): The identifier of the DB instance to promote.
        
        Returns:
        - `db_instance` (DBInstance): The promoted DB instance.
        
        Raises:
        - `Exception`: If the instance is not a replica or if its status is not available.
        '''
        db_instance = self.get(db_instance_identifier)
        if not db_instance.is_replica:
            raise Exception("failover is not supported for master instances.")
        if db_instance.status != 'available':
            raise Exception(f"DB instance {db_instance_identifier} is not available.")
        setattr(db_instance,'is_replica',False)
        self.dal.modifyDBInstance(db_instance)
        return db_instance
    
    def promote_read_replica(self, db_instance_identifier):
        '''Promotes a read replica to become the primary instance in new cluster.
        
        Parameters:
        - `db_instance_identifier` (str): The identifier of the DB instance to promote.
        
        Returns:
        - `db_instance` (DBInstance): The promoted DB instance.
        
        Raises:
        - `Exception`: If the instance is not a replica or if its status is not available.
        '''
        db_instance = self.get(db_instance_identifier)
        if not db_instance.is_replica:
            raise Exception("Promote is not supported for master instances.")
        if db_instance.status != 'available':
            raise Exception(f"DB instance {db_instance_identifier} is not available.")
        self.remove_replica_from_cluster(db_instance)
        setattr(db_instance,'is_replica',False)
        from DB.NEW_KT_DB.Controller.DBClusterController import DBClusterController,DBClusterService,DBClusterManager,DBInstanceController
        db_cluster_controller=DBClusterController(DBClusterService(DBClusterManager(self.dal.object_manager),self.storageManager,DBInstanceModel.BASE_PATH),DBInstanceController(self))
        db_cluster_identifier=str(uuid.uuid4())
        db_cluster_controller.create_db_cluster(db_cluster_identifier='a'+(db_cluster_identifier) ,engine= 'mysql',allocated_storage=5,db_subnet_group_name= 'my-subnet-group')
        setattr(db_instance,'db_cluster_identifier',db_cluster_identifier)
        self.dal.modifyDBInstance(db_instance)
        return db_instance

    def create_snapshot(self, db_instance_identifier, db_snapshot_identifier,from_create=False):
        db_instance = self.get(db_instance_identifier)
        setattr(db_instance,'created_time', datetime.now())
        db_instance._node_subSnapshot_name_to_id[db_snapshot_identifier] = db_instance._last_node_of_current_version.id_snapshot
        if not from_create:
            self.send_to_cluster(db_instance._last_node_of_current_version,db_snapshot_identifier)
        self._create_child_to_node(db_instance)
        self.dal.modifyDBInstance(db_instance)
        return db_instance

    def delete_snapshot(self, db_instance_identifier, db_snapshot_identifier):
        db_instance = self.get(db_instance_identifier)
        if db_instance.is_replica:
            raise Exception("Delete snapshot is not supported for read replicas.")
        self.delete_snapshot_from_replica(db_instance_identifier,db_snapshot_identifier,db_instance)

    def delete_snapshot_from_replica(self,db_instance_identifier,db_snapshot_identifier,db_instance=None):
        '''
        Deletes a snapshot from a DB instance replica.
        
        Parameters:
        - `db_instance_identifier` (str): The identifier of the DB instance.
        - `db_snapshot_identifier` (str): The identifier of the snapshot to delete.
        - `db_instance` (DBInstance, optional): The DB instance object. If not provided, it will be fetched using `db_instance_identifier`.
        
        Raises:
        - `DbSnapshotIdentifierNotFoundError`: If the specified snapshot identifier is not found.
        '''
        if db_instance is None:
            db_instance = self.get(db_instance_identifier)
        if db_snapshot_identifier not in db_instance._node_subSnapshot_name_to_id:
            raise DbSnapshotIdentifierNotFoundError(f"Snapshot '{db_snapshot_identifier}' not found for instance '{db_instance.db_instance_identifier}'")
        if not db_instance.is_replica:
            self.send_cluster_remove_snapeshot(db_instance.db_instance_identifier,db_snapshot_identifier)
        db_instance._node_subSnapshot_name_to_id.pop(db_snapshot_identifier, None)
        self.dal.modifyDBInstance(db_instance)

    def restore_version(self, db_instance_identifier, db_snapshot_identifier,db_instance=None):
        '''
        Restores a DB instance to a specific snapshot.
        
        Parameters:
        - `db_instance_identifier` (str): The identifier of the DB instance to restore.
        - `db_snapshot_identifier` (str): The identifier of the snapshot to restore.
        - `db_instance` (DBInstance, optional): The DB instance object. If not provided, it will be fetched using `db_instance_identifier`.
        
        Raises:
        - `DbSnapshotIdentifierNotFoundError`: If the specified snapshot identifier is not found.
        
        Returns:
        - `db_instance` (DBInstance): The restored DB instance.
        '''
                
        if db_instance is None:
            db_instance = self.get(db_instance_identifier)

        if db_snapshot_identifier not in db_instance._node_subSnapshot_name_to_id:
            raise DbSnapshotIdentifierNotFoundError(
                f"Snapshot identifier '{db_snapshot_identifier}' not found.")

        node_id = db_instance._node_subSnapshot_name_to_id[db_snapshot_identifier]
        snapshot_id_uuid = uuid.UUID(node_id)
        snapshot = db_instance._node_subSnapshot_dic.get(snapshot_id_uuid)
        if not db_instance.is_replica:
            self.send_to_cluster_restore_version(db_instance_identifier,db_snapshot_identifier)
        if snapshot:
            self._update_queue_to_current_version(snapshot, db_instance)
            self._create_child_to_node(db_instance)


        self.dal.modifyDBInstance(db_instance)

        return db_instance

    def restore_version_snapshot(self, db_instance_identifier, db_snapshot_identifier):
        db_instance = self.get(db_instance_identifier)  
        if db_instance.is_replica: 
            raise Exception("Switchover is not supported for read replicas.")
        return self.restore_version(db_instance_identifier,db_snapshot_identifier,db_instance)

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
        if db_instance.is_replica:
            raise Exception("write to databases is not supported for master instances.")
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

    def _validate_parameters(self, **kwargs):
        """
        Validates the parameters passed to a function or method.
        
        Args:
            allocated_storage (int): The allocated storage for the database instance.
            master_user_password (str): The password for the master user.
            port (int): The port number for the database instance.
            master_user_name (str): The username for the master user.
        
        Raises:
            ValueError: If any of the parameters are invalid.
        """
        if 'allocated_storage' in kwargs:
            validate_allocated_storage(kwargs['allocated_storage'])

        if 'master_user_password' in kwargs:
            validate_master_user_password(kwargs['master_user_password'])

        if 'port' in kwargs:
            validate_port(kwargs['port'])
            
        if 'master_user_name'in kwargs:
            validate_master_user_name(kwargs['master_user_name'])

    def copy_snapshot_to_instance(self, source_snapshot: Node_SubSnapshot, target_db_instance_identifier: str,db_snapshot_identifier: str):
        """
        Copies a database snapshot from one DB instance to another.
        
        Args:
            source_snapshot (Node_SubSnapshot): The source database snapshot to copy.
            target_db_instance_identifier (str): The identifier of the target DB instance to copy the snapshot to.
            db_snapshot_identifier (str): The identifier of the database snapshot to copy.
        
        Raises:
            Exception: If the target DB instance is not a replica instance.
        
        Returns:
            DBInstanceModel: The updated target DB instance after the snapshot copy.
        """
        target_db_instance = self.get(target_db_instance_identifier)
        if not target_db_instance.is_replica:
            raise Exception("copy snapshot is not supported for master instances.")
        # Create a new Node_SubSnapshot for the target instance
        new_snapshot =self._copy_snapshot(source_snapshot,target_db_instance.endpoint)

        # Update the target DBInstance
        target_db_instance._node_subSnapshot_name_to_id[db_snapshot_identifier] = str(source_snapshot.id_snapshot)
        target_db_instance._node_subSnapshot_dic[str(new_snapshot.id_snapshot)] = new_snapshot
        target_db_instance._current_version_ids_queue.append(str(new_snapshot.id_snapshot))
        target_db_instance._last_node_of_current_version = new_snapshot

        # Save the changes to the database
        self.dal.modifyDBInstance(target_db_instance)

        return target_db_instance

    def send_to_cluster(self,last_node:Node_SubSnapshot,db_snaapshot_identifier):
        pass     
    
    def send_cluster_remove_snapeshot(self,db_instance_identifier,db_snapshot_identifier):
        pass

    def send_to_cluster_restore_version(self,db_instance_identifier,db_snapshot_identifier):
        pass
    
    def add_replica_to_cluster(self,db_instance:DBInstanceModel):
        pass

    def remove_replica_from_cluster(self,db_instance:DBInstanceModel):
        pass

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
