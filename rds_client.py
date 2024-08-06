from db_instance import *
from exception import *
from sql_commands import *
import shutil
from datetime import datetime
from help_functions import get_json
from validation import check_required_params, check_extra_params

db_instances = {}
db_snapshots = {}


def wake_up_object(object_id, class_name, metadata, parent_id):
    """build an object from its attributes
    args: object id, the class the object is instance of, dict of all its attributes"""
    if class_name == "DBInstance":
        instance = DBInstance(**metadata)
        db_instances[object_id] = instance
    elif class_name == "DBSnapshot":
        db_snapshots[object_id] = metadata


def get_instance_by_id(instance_id):
    """Find a DB instance by its identifier."""
    if instance_id not in db_instances:
        raise DBInstanceNotFoundError('Instance with that id does not exist')
    return db_instances[instance_id]


def create_db_instance(**kwargs):
    """Create a new DB instance."""
    required_params = ['db_instance_identifier', 'allocated_storage', 'master_username', 'master_user_password']
    all_params = ['db_name', 'port'].extend(required_params)
    check_required_params(required_params, kwargs)  # check if there are all required parameters
    check_extra_params(all_params, kwargs)
    if kwargs['db_instance_identifier'] in db_instances:  # check if db id is unique
        raise AlreadyExistsError("Your DB instance identifier already exists")

    instance = DBInstance(**kwargs)  # build object
    instance.save_to_db()  # save it in the management object table
    db_instances[kwargs['db_instance_identifier']] = instance  # save pointer to object
    return {DBInstance: instance.get_data_dict()}  # return describe of object


def delete_db_instance(**kwargs):
    """Delete a DB instance."""
    required_params = ['db_instance_identifier']
    all_params = ['SkipFinalSnapshot', 'FinalDBSnapshotIdentifier', 'DeleteAutomatedBackups'].extend(required_params)
    check_required_params(required_params, kwargs)  # check if there are all required parameters
    check_extra_params(all_params, kwargs)
    if 'skip_final_snapshot' not in kwargs or kwargs['skip_final_snapshot'] == False:
        if 'FinalDBSnapshotIdentifier' not in kwargs:
            raise ParamValidationError(
                "If you don’t enable skip_final_snapshot parameter, you must specify the FinalDBSnapshotIdentifier parameter.")
    db_id = kwargs['db_instance_identifier']
    if db_id not in db_instances:  # check if db to delete exists
        raise DBInstanceNotFoundError("This DB instance identifier doesn't exist")
    if 'skip_final_snapshot' in kwargs and kwargs['skip_final_snapshot']:
        create_db_snapshot(db_instance_identifier=kwargs['db_instance_identifier'])
    del_object(db_id, 'object_management', 'object_management.db')
    endpoint = db_instances[db_id].get_endpoint()
    if os.path.exists(endpoint): #delete db instance directory
        shutil.rmtree(endpoint)
    del db_instances[db_id]


def stop_db_instance(**kwargs):
    """Stop a DB instance."""
    required_params = ['db_instance_identifier']
    check_required_params(required_params, kwargs)
    db_id = kwargs['db_instance_identifier']
    if db_id not in db_instances:
        raise DBInstanceNotFoundError("This DB instance identifier doesn't exist")
    if 'FinalDBSnapshotIdentifier' in kwargs:
        create_db_snapshot(DBInstanceIdentifier=db_id, DBSnapshotIdentifier=kwargs['DBSnapshotIdentifier'])
    db_instances[db_id].stop()
    instance_to_stop = db_instances[db_id]
    del db_instances[db_id]
    return instance_to_stop.get_data_dict()


def start_db_instance(**kwargs):
    """Start a DB instance."""
    required_params = ['db_instance_identifier']
    check_required_params(required_params, kwargs)
    db_id = kwargs['db_instance_identifier']
    if check_if_exists_in_table('object_management', 'object_management', db_id):
        raise DBInstanceNotFoundError("db not exist")
    row = get_object_from_table_by_id('object_management', 'object_management', db_id)
    id_of_db, class_name, metadata = row
    wake_up_object(id_of_db, class_name, metadata)
    db_instances[db_id].start()
    return db_instances[db_id].get_data_dict()


def describe_events(**kwargs):
    """Describe events related to the DB instances."""
    pass


def comments():
    # def create_db_snapshot1(**kwargs):
    #     """Create a snapshot of a DB instance."""
    #     required_params = ['DBSnapshotIdentifier', 'DBInstanceIdentifier']
    #     check_required_params(required_params, kwargs)
    #     if kwargs['DBInstanceIdentifier'] not in db_instances:
    #         raise DBInstanceNotFoundError("DB instance identifier not found")
    #     if kwargs['DBInstanceIdentifier'] not in db_snapshots:
    #         create_base_snapshot(kwargs['DBInstanceIdentifier'], kwargs['DBSnapshotIdentifier'])
    #     else:
    #         create_diff_snapshot(kwargs['DBInstanceIdentifier'], kwargs['DBSnapshotIdentifier'])
    #
    #
    # def create_base_snapshot(db_instance_identifier, snapshot_identifier):
    #     db_instance = db_instances[db_instance_identifier]
    #     snapshot_dir = os.path.join(db_instance.endpoint, "snapshots")
    #
    #     # Create the snapshots directory if it doesn't exist
    #     if not os.path.exists(snapshot_dir):
    #         os.makedirs(snapshot_dir)
    #
    #     # Create a unique filename for the base snapshot
    #     base_snapshot_path = os.path.join(snapshot_dir, f"{snapshot_identifier}.tar.gz")
    #
    #     # Compress the entire DB folder
    #     shutil.make_archive(base_snapshot_path.replace(".tar.gz", ""), 'gztar', db_instance.endpoint)
    #
    #     # Store snapshot metadata
    #     snapshot_metadata = {
    #         'db_instance_identifier': db_instance_identifier,
    #         'snapshot_identifier': snapshot_identifier,
    #         'created_time': datetime.now().isoformat(),
    #         'snapshot_path': base_snapshot_path,
    #         'type': 'base'
    #     }
    #
    #     db_snapshots[db_instance_identifier] = {}
    #     db_snapshots[db_instance_identifier][snapshot_identifier] = snapshot_metadata
    #     db_snapshots[db_instance_identifier]["base"] = snapshot_identifier
    #     insert_into_management_table("DBSnapshot", snapshot_identifier, get_json(snapshot_metadata), db_instance_identifier)
    #     insert_into_management_table("DBSnapshot", "base", snapshot_identifier, db_instance_identifier)
    #     print(f"Base snapshot {snapshot_identifier} created successfully at {base_snapshot_path}")
    #
    #
    # def create_diff_snapshot(db_instance_identifier, snapshot_identifier):
    #     if "base" not in db_snapshots[db_instance_identifier]:
    #         raise ValueError("Cant create diff snapshot without base before")
    #     base_snapshot_identifier = db_snapshots[db_instance_identifier]["base"]
    #     if base_snapshot_identifier not in db_snapshots[db_instance_identifier]:
    #         raise ValueError("Base snapshot identifier not found")
    #
    #     db_instance = db_instances[db_instance_identifier]
    #     snapshot_dir = os.path.join(db_instance.endpoint, "snapshots")
    #
    #     # Create a unique filename for the diff snapshot
    #     diff_snapshot_path = os.path.join(snapshot_dir, f"{snapshot_identifier}.tar.gz")
    #
    #     # Identify the changes (for simplicity, let's assume we use file timestamps)
    #     base_snapshot_time = datetime.fromisoformat(
    #         db_snapshots[db_instance_identifier][base_snapshot_identifier]['created_time'])
    #
    #     for root, dirs, dbs in os.walk(db_instance.endpoint):
    #         for db in dbs:
    #             db_connection = os.path.join(root, db)
    #             changes = fetch_changes_since(db_connection, base_snapshot_time)
    #             db_snapshot_path = os.path.join(diff_snapshot_path, db)
    #             with open(db_snapshot_path, "w") as ds:
    #                 json.dump(changes, ds)
    #     prev_snapshot_id = db_snapshots[db_instance_identifier]["prev"] if "prev" in db_snapshots[
    #         db_instance_identifier] else None
    #
    #     # Store snapshot metadata
    #     snapshot_metadata = {
    #         'db_instance_identifier': db_instance_identifier,
    #         'snapshot_identifier': snapshot_identifier,
    #         'created_time': datetime.now().isoformat(),
    #         'snapshot_path': diff_snapshot_path,
    #         'base_snapshot': base_snapshot_identifier,
    #         'prev_snapshot': prev_snapshot_id,
    #         'type': 'diff'
    #     }
    #     snapshot_metadata = {k: v for k, v in snapshot_metadata if v is not None}
    #     db_snapshots[db_instance_identifier]["prev"] = snapshot_identifier
    #     insert_into_management_table("DBSnapshot", "prev", snapshot_identifier, db_instance_identifier)
    #
    #     db_snapshots[db_instance_identifier][snapshot_identifier] = snapshot_metadata
    #     insert_into_management_table("DBSnapshot", snapshot_identifier, get_json(snapshot_metadata), db_instance_identifier)
    #     print(f"Diff snapshot {snapshot_identifier} created successfully at {diff_snapshot_path}")
    #
    #
    # def backup_changes(database, backup_dir, last_backup_time):
    #     connection = sqlite3.connect(database)
    #     cursor = connection.cursor()
    #
    #     try:
    #         cursor.execute("SELECT * FROM change_log WHERE change_time > ?", (last_backup_time,))
    #         changes = cursor.fetchall()
    #
    #         if changes:
    #             backup_file = os.path.join(backup_dir, f"change_log_backup_{datetime.now().strftime('%Y%m%d%H%M%S')}.json")
    #             with open(backup_file, 'w') as f:
    #                 json.dump(changes, f)
    #             print(f"Backup saved to {backup_file}")
    #
    #
    #     finally:
    #         connection.close()
    #
    #
    # def restore_changes(database, backup_file):
    #     connection = sqlite3.connect(database)
    #     cursor = connection.cursor()
    #
    #     try:
    #         with open(backup_file, 'r') as f:
    #             changes = json.load(f)
    #             for change in changes:
    #                 table_name = change[1]
    #                 operation = change[2]
    #                 row_id = change[3]
    #                 old_data = json.loads(change[4]) if change[4] else {}
    #                 new_data = json.loads(change[5]) if change[5] else {}
    #
    #                 if operation == 'INSERT':
    #                     columns = ', '.join(new_data.keys())
    #                     values = ', '.join(['?'] * len(new_data))
    #                     cursor.execute(f"INSERT INTO {table_name} ({columns}) VALUES ({values})", tuple(new_data.values()))
    #                 elif operation == 'UPDATE':
    #                     updates = ', '.join([f"{k} = ?" for k in new_data.keys()])
    #                     cursor.execute(f"UPDATE {table_name} SET {updates} WHERE id = ?", (*new_data.values(), row_id))
    #                 elif operation == 'DELETE':
    #                     cursor.execute(f"DELETE FROM {table_name} WHERE id = ?", (row_id,))
    #
    #         connection.commit()
    #
    #     finally:
    #         connection.close()
    pass ##


def create_db_snapshot(**kwargs):
    required_params = ['db_instance_identifier', 'db_snapshot_identifier']
    check_required_params(required_params, kwargs)
    db_instance_identifier, snapshot_identifier = kwargs['db_instance_identifier'], kwargs['db_snapshot_identifier']
    if db_instance_identifier not in db_instances:
        raise DBInstanceNotFoundError("DB instance identifier not found")
    if snapshot_identifier in db_snapshots:
        raise AlreadyExistsError("snapshot identifier already exist")

    db_instance = db_instances[db_instance_identifier]
    snapshot_dir = f'{db_instance.endpoint}_snapshots'

    if not os.path.exists(snapshot_dir):
        os.makedirs(snapshot_dir)

    snapshot_path = os.path.join(snapshot_dir, f"{snapshot_identifier}.tar.gz")

    shutil.make_archive(snapshot_path.replace(".tar.gz", ""), 'gztar', db_instance.endpoint)
    snapshot_databases = {db_name: os.path.join(snapshot_dir, db_name) for db_name in
                          db_instance.databases.keys()}

    snapshot_metadata = {
        'db_instance_identifier': db_instance_identifier,
        'snapshot_identifier': snapshot_identifier,
        'created_time': datetime.now().isoformat(),
        'snapshot_path': snapshot_path,
        'databases': snapshot_databases,
        'allocated_storage': db_instance.allocated_storage,
        'master_username': db_instance.master_username,
        'master_user_password': db_instance.master_user_password
    }
    if db_instance_identifier not in db_snapshots:
        db_snapshots[db_instance_identifier] = {}

    db_snapshots[snapshot_identifier] = snapshot_metadata
    insert_into_management_table('DBSnapshot', snapshot_identifier, get_json(snapshot_metadata), db_instance_identifier)
    print(f"Snapshot {snapshot_identifier} created successfully at {snapshot_path}")


def restore_db_instance_from_snapshot(**kwargs):
    required_params = ['db_instance_identifier', 'db_snapshot_identifier']
    check_required_params(required_params, kwargs)
    db_instance_identifier, snapshot_identifier = kwargs['db_instance_identifier'], kwargs['db_snapshot_identifier']
    if snapshot_identifier not in db_snapshots:
        raise ValueError("Snapshot identifier not found")

    snapshot_metadata = db_snapshots[snapshot_identifier]

    if db_instance_identifier in db_instances:
        raise AlreadyExistsError("DB instance identifier already exists")

    new_instance_endpoint = os.path.join(DBInstance.BASE_PATH, db_instance_identifier)
    os.makedirs(new_instance_endpoint)

    snapshot_path = snapshot_metadata['snapshot_path']
    shutil.unpack_archive(snapshot_path, new_instance_endpoint, 'gztar')
    new_db_databases = {db_name: os.path.join(new_instance_endpoint, db_name) for db_name in
                        snapshot_metadata['databases'].keys()}

    new_db_instance = DBInstance(
        db_instance_identifier=db_instance_identifier,
        allocated_storage=snapshot_metadata['allocated_storage'],
        master_username=snapshot_metadata['master_username'],
        master_user_password=snapshot_metadata['master_user_password'],
        databases=new_db_databases
    )
    new_db_instance.endpoint = new_instance_endpoint
    db_instances[db_instance_identifier] = new_db_instance
    new_db_instance.save_to_db()

    print(f"DB instance {db_instance_identifier} restored successfully from snapshot {snapshot_identifier}")


def copy_db_snapshot(**kwargs):
    """Copy a DB snapshot."""
    pass


def restore_db_instance_from_s3(**kwargs):
    """Restore a DB instance from an S3 backup."""
    pass


def download_db_log_file_portion(**kwargs):
    """Download a portion of the DB log file."""
    pass
