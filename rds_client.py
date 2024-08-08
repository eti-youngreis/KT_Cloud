from db_instance import *
from exception import *
from sql_commands import *
import shutil
from datetime import timedelta
from datetime import datetime
from help_functions import get_json
from validation import check_required_params, check_extra_params, check_filters_validation
import json
from event import log_event, get_event_by_func_name
from sql_commands_for_log_event_table import get_events_from_table_by_start_and_end_times
db_instances = {}
db_snapshots = {}


def wake_up_object(object_id, class_name, metadata, parent_id):
    """build an object from its attributes
    args: object id, the class the object is instance of, dict of all its attributes"""
    if class_name == 'DBInstance':
        instance = DBInstance(**metadata)
        db_instances[object_id] = instance
    elif class_name == 'DBSnapshot':
        db_snapshots[object_id] = metadata


def get_instance_by_id(instance_id):
    """Find a DB instance by its identifier."""
    if instance_id not in db_instances:
        raise DBInstanceNotFoundError('Instance with that id does not exist')
    return db_instances[instance_id]

@log_event
def create_db_instance(**kwargs):
    """Create a new DB instance."""
    required_params = ['db_instance_identifier', 'allocated_storage', 'master_username', 'master_user_password']
    all_params = ['db_name', 'port']
    all_params.extend(required_params)
    check_required_params(required_params, kwargs)  # check if there are all required parameters
    check_extra_params(all_params, kwargs) #check if there are not extra params that the function can't get
    if kwargs['db_instance_identifier'] in db_instances:  # check if db id is unique
        raise AlreadyExistsError('Your DB instance identifier already exists')

    instance = DBInstance(**kwargs)  # build object
    instance.save_to_db()  # save it in the management object table
    db_instances[kwargs['db_instance_identifier']] = instance  # save pointer to object
    return {DBInstance: instance.get_data_dict()}  # return describe of object

@log_event
def delete_db_instance(**kwargs):
    """Delete a DB instance."""
    required_params = ['db_instance_identifier']
    all_params = ['skip_final_snapshot', 'final_db_snapshot_identifier', 'delete_automated_backups']
    all_params.extend(required_params)
    check_required_params(required_params, kwargs)  # check if there are all required parameters
    check_extra_params(all_params, kwargs) #check if there are not extra params that the function can't get
    db_id = kwargs['db_instance_identifier']
    if db_id not in db_instances:  # check if db to delete exists
        raise DBInstanceNotFoundError('This DB instance identifier does not exist')
    if 'skip_final_snapshot' not in kwargs or kwargs['skip_final_snapshot'] == False: #if need to do final snapshot
        if 'final_db_snapshot_identifier' not in kwargs: #raise when snapshot id was not given
            raise ParamValidationError(
                'If you do not enable skip_final_snapshot parameter, you must specify the FinalDBSnapshotIdentifier parameter')
        create_db_snapshot(db_instance_identifier=kwargs['db_instance_identifier'], #create final snapshot
                           db_snapshot_identifier=kwargs['final_db_snapshot_identifier'])

    del_object(db_id, 'object_management', 'object_management.db')
    endpoint = db_instances[db_id].get_endpoint()
    if os.path.exists(endpoint):  # delete db instance directory
        shutil.rmtree(endpoint)
    del db_instances[db_id]

@log_event
def stop_db_instance(**kwargs):
    """Stop a DB instance from running"""
    required_params = ['db_instance_identifier']
    all_params = ['final_db_snapshot_identifier', 'skip_final_snapshot', 'delete_automated_backups']
    all_params.extend(required_params)
    check_required_params(required_params, kwargs) # check if there are all required parameters
    check_extra_params(all_params, kwargs) #check if there are not extra params that the function can't get
    db_id = kwargs['db_instance_identifier'] 
    if db_id not in db_instances: # check if db to start exists
        raise DBInstanceNotFoundError('This DB instance identifier does not exist')
    if 'final_db_snapshot_identifier' in kwargs: #if need to create snapshot
        create_db_snapshot(DBInstanceIdentifier=db_id, DBSnapshotIdentifier=kwargs['final_db_snapshot_identifier'])
    db_instances[db_id].stop()
    instance_to_stop = db_instances[db_id]
    del db_instances[db_id] #stop running of db object
    return instance_to_stop.get_data_dict()

@log_event
def start_db_instance(**kwargs):
    """Start a DB instance."""
    required_params = ['db_instance_identifier']
    check_required_params(required_params, kwargs)
    db_id = kwargs['db_instance_identifier']
    if not check_if_exists_in_table('object_management.db', 'object_management', db_id):
        raise DBInstanceNotFoundError("db not exist")
    if db_id in db_instances:
        raise StartNonStoppedDBInstance('cannot restart non stopped instance')
    row = get_object_from_table_by_id('object_management.db', 'object_management', db_id)
    class_name, id_of_db, metadata, parent_id = row
    metadata = json.loads(metadata)
    # return object to run
    wake_up_object(id_of_db, class_name, metadata, parent_id)
    db_instances[db_id].start()
    return db_instances[db_id].get_data_dict()


@log_event
def create_db_snapshot(**kwargs):
    """create a snapshot of a given db instance"""
    required_params = ['db_instance_identifier', 'db_snapshot_identifier']
    all_params = ['tags']
    all_params.extend(required_params)
    check_required_params(required_params, kwargs)
    check_extra_params(all_params, kwargs)
    db_instance_identifier, snapshot_identifier = kwargs['db_instance_identifier'], kwargs['db_snapshot_identifier']
    #check if db instance for the snapshot exists
    if db_instance_identifier not in db_instances:
        raise DBInstanceNotFoundError('DB instance identifier not found')
    #check if snapshot id is unique
    if snapshot_identifier in db_snapshots:
        raise AlreadyExistsError('snapshot identifier already exist')

    db_instance = db_instances[db_instance_identifier]
    #create snapshot directory
    snapshot_dir = f'{db_instance.endpoint}_snapshots'

    if not os.path.exists(snapshot_dir):
        os.makedirs(snapshot_dir)

    snapshot_path = os.path.join(snapshot_dir, f'{snapshot_identifier}.tar.gz')

    #copy db instance files
    shutil.make_archive(snapshot_path.replace('.tar.gz', ''), 'gztar', db_instance.endpoint)
    snapshot_databases = {db_name: os.path.join(snapshot_dir, db_name) for db_name in
                          db_instance.databases.keys()}
    
    #create and save snapshot metadata
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
    print(f'Snapshot {snapshot_identifier} created successfully at {snapshot_path}')

@log_event
def restore_db_instance_from_snapshot(**kwargs):
    """restore a db from a given snapshot"""
    required_params = ['db_instance_identifier', 'db_snapshot_identifier']
    check_required_params(required_params, kwargs)
    db_instance_identifier, snapshot_identifier = kwargs['db_instance_identifier'], kwargs['db_snapshot_identifier']

    if snapshot_identifier not in db_snapshots:
        raise DBSnapshotNotFoundError('Snapshot identifier not found')

    snapshot_metadata = db_snapshots[snapshot_identifier]

    if db_instance_identifier in db_instances:
        raise AlreadyExistsError('DB instance identifier already exists')
    
    #create db instance directory
    new_instance_endpoint = os.path.join(DBInstance.BASE_PATH, db_instance_identifier)
    os.makedirs(new_instance_endpoint)

    snapshot_path = snapshot_metadata['snapshot_path']
    #copy snapshot files 
    shutil.unpack_archive(snapshot_path, new_instance_endpoint, 'gztar')
    new_db_databases = {db_name: os.path.join(new_instance_endpoint, db_name) for db_name in
                        snapshot_metadata['databases'].keys()}
    #create db instance object
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

    print(f'DB instance {db_instance_identifier} restored successfully from snapshot {snapshot_identifier}')



def describe_events(**kwargs):
    """Describe events related to the DB instances."""
    max_event_days = 14
    # params validation
    all_params = [ 'source_identifier','source_type','start_time','end_time','duration','event_categories','filters''max_records','marker']
    check_extra_params(all_params, kwargs)

    # put params to variables
    source_id = kwargs.get('source_identifier',None)
    src_type = kwargs.get('source_type',None)
    start_time = kwargs.get('start_time',None)
    end_time = kwargs.get('end_time',None)
    duration = kwargs.get('duration',None)
    event_categories = kwargs.get('event_categories',None)
    filters = kwargs.get('filters',None)
    max_records = kwargs.get('max_records',None)
    marker = kwargs.get('marker',None)

    # params validation
    if not not duration == start_time and end_time:
        raise ParamValidationError('can get only duration or only start time and end time')
    if source_id and not src_type:
        raise ParamValidationError('If source_identifier is supplied, source_type must also be provided.')
    if src_type and src_type not in ['db-instance','db-parameter-group','db-security-group','db-snapshot','db-cluster','db-cluster-snapshot','custom-engine-version','db-proxy','blue-green-deployment']:
        raise ParamValidationError('source type must be one of [db-instance,db-parameter-group,db-security-group,db-snapshot,db-cluster,db-cluster-snapshot,custom-engine-version,db-proxy,blue-green-deployment]')
    if filters:
        check_filters_validation(kwargs['filters'])
    
    # set start time and end time, by default 14 days ago and today
    if kwargs.get('duration'):
        if kwargs.get('start_time'):
            start_time = kwargs['start_time']
            end_time = start_time + timedelta(minutes=duration)
        elif kwargs.get('end_time'):
            start_time = kwargs['start_time']
            end_time = start_time + timedelta(minutes=duration)
        else:
            end_time = datetime.now()
            start_time = end_time - timedelta(minutes=duration)
    if not start_time:
        start_time = datetime.now() - timedelta(days=max_event_days)
    if not end_time:
        end_time = datetime.now()
    
    events = get_events_from_table_by_start_and_end_times(start_time.strftime('%Y-%m-%d %H:%M:%S'), end_time.strftime('%Y-%m-%d %H:%M:%S'))
    events_to_return = []
    for event in events:
        event_id, resource_id, date = event['event_id'] ,event['resource_id'], event['created_time']
        event_data = get_event_by_func_name(True,event_id)
        event_to_return = {
            'source_identifier':resource_id,
            'source_type':event_data['resource_type'],
            'message': event_data['message'],
            'event_category': event_data['category'],
            'date': datetime.strptime(date, '%Y-%m-%d %H:%M:%S')

        }
        events_to_return.append(event_to_return)
    if source_id:
        events_to_return = [event for event in events_to_return if event['source_identifier'] == source_id and event['source_type'] == src_type]
    if event_categories:
        events_to_return = [event for event in events_to_return if event['event_category'] in event_categories]
    return events_to_return


def copy_db_snapshot(**kwargs):
    """Copy a DB snapshot."""
    required_params = ['source_db_snapshot_identifier', 'target_db_snapshot_identifier']
    all_params = ['tags','copy_tags','option_group_name','TargetCustomAvailabilityZone',
    'CopyOptionGroup','SourceRegion']
    all_params.extend(required_params)
    check_required_params(required_params, kwargs)
    check_extra_params(all_params, kwargs)
    pass


def restore_db_instance_from_s3(**kwargs):
    """Restore a DB instance from an S3 backup."""
    pass


def download_db_log_file_portion(**kwargs):
    """Download a portion of the DB log file."""
    pass
