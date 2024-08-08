# from parameter_group import ParameterGroup
import re
from DB.Scripts.db_parameter_group import DBParameterGroup
from DB.Scripts.db_cluster_parameter_group import DBClusterParameterGroup
from typing import List
from DB.Scripts.Management import *
from DB.Scripts.db_cluster import DBCluster

clusters: List[DBCluster] = []
db_parameter_groups: List[DBParameterGroup] = []
db_cluster_parameter_groups: List[DBClusterParameterGroup] = []


def delete_db_instance(instance_id):
    """
    Function to delete a record from the Management table by name and ID

    Args:
    instance_id (str): The ID of the DBInstance record to delete

    Returns:
    bool: True if the deletion was successful, False otherwise
    """
    delete_from_management("DBInstance", instance_id)


def find_cluster_and_endpoint_by_endpoint_identifier(endpoint_identifier, clusters_in_func):
    """
    Function to find a cluster and endpoint by endpoint identifier

    Args:
    endpoint_identifier (str): The identifier of the endpoint to search for
    clusters_in_func (list): List of clusters to search within

    Returns:
    tuple: The found cluster and endpoint

    Raises:
    ValueError: If the endpoint with the given identifier is not found in any cluster
    """
    for cluster in clusters_in_func:
        for endpoint in cluster.endpoints:
            if endpoint.endpoint_identifier == endpoint_identifier:
                return cluster, endpoint
    raise ValueError(f"Endpoint with identifier {endpoint_identifier} not found in any cluster")


def delete_db_snapshot(snapshot_id):
    """
    Function to delete a record from the Management table by name and ID

    Args:
    snapshot_id (str): The ID of the DBSnapshot record to delete

    Returns:
    bool: True if the deletion was successful, False otherwise
    """
    delete_from_management("DBSnapshot", snapshot_id)


def get_dbcluster_by_id(cluster_id, clusters_in_func=clusters):
    """
    Function to find a cluster by its ID

    Args:
    cluster_id (str): The ID of the cluster to search for
    clusters_in_func (list): List of clusters to search within

    Returns:
    DBCluster: The found cluster

    Raises:
    ValueError: If the cluster does not exist
    """
    cluster = next(
        (c for c in clusters_in_func if c.cluster_identifier == cluster_id), None)
    if cluster is None:
        raise ValueError("Cluster does not exist")
    return cluster


def create_parameter_group(parameter_groups, module_name, class_name, parameter_group_name, parameter_group_family,
                           description, tags, conn=None):
    """
    Function to create a parameter group

    Args:
    parameter_groups (list): List of parameter groups to check against
    module_name (str): The module name of the parameter group
    class_name (str): The class name of the parameter group
    parameter_group_name (str): The name of the parameter group
    parameter_group_family (str): The family of the parameter group
    description (str): Description of the parameter group
    tags (list): Tags associated with the parameter group

    Returns:
    dict: The description of the created parameter group

    Raises:
    ValueError: If the parameter group already exists
    """
    if bool(re.match('^[a-z]+$', parameter_group_name)):
        raise ValueError("the parameter_group_name must be with lower letters")
    parameter_group_names = [
        p.db_cluster_parameter_group_name for p in parameter_groups]
    if parameter_group_name in parameter_group_names:
        raise ValueError(f"The parameter group already exists")
    parameter_group = DBParameterGroup(module_name, class_name, parameter_group_name, parameter_group_family, description,
                                     tags)
    parameter_group.save_to_db(conn)
    db_parameter_groups.append(parameter_group)
    return parameter_group.describe()


def describe_parameter_groups(parameter_groups, title, parameter_group_name, max_records, marker):
    """
    Function to describe parameter groups

    Args:
    parameter_groups (list): List of parameter groups to describe
    title (str): Title of the result
    parameter_group_name (str, optional): Name of the parameter group to search for (default: None)
    max_records (int, optional): Maximum number of records to return (default: 100)
    marker (str, optional): Marker to start the description from (default: None)

    Returns:
    dict: A dictionary describing the found parameter groups
    """
    parameter_groups_local = []
    if parameter_group_name is not None:
        parameter_group = next((p for p in parameter_groups if
                                p.parameter_group_name == parameter_group_name), None)
        if parameter_group is None:
            raise ValueError(f"The parameter group does not exist")
        parameter_groups_local.append(parameter_group.describe(False))
    else:
        count = 0
        for p in parameter_groups:
            if p.parameter_group_name == marker or marker is None:
                marker = None
                count += 1
                if count <= max_records:
                    parameter_groups_local.append(p.describe(False))
                else:
                    marker = p.parameter_group_name
    if marker is None:
        return {title: parameter_groups_local}
    return {'Marker': marker, title: parameter_groups_local}


def describe_parameters(parameter_groups, parameter_group_name, source, max_records, marker):
    """
    Retrieves a list of parameters from a specific parameter group, filtered by source and limited by max_records.

    Args:
        parameter_groups (list): A list of parameter groups to search through.
        parameter_group_name (str): The name of the parameter group to retrieve parameters from.
        source (str): The source of the parameters to return (e.g., 'user', 'system', 'engine-default').
        max_records (int): The maximum number of records to return in the response.
        marker (str, optional): The marker to use for pagination. If None, starts from the beginning.

    Returns:
        dict: A dictionary containing:
            - 'Parameters': A list of parameters matching the criteria.
            - 'Marker' (optional): A marker indicating the position to resume pagination if there are more records.
    """
    parameter_group = next(
        (p for p in parameter_groups if p.parameter_group_name == parameter_group_name), None)
    if parameter_group is None:
        raise ValueError(f"The parameter group does not exist")

    parameters_local = []
    count = 0
    for p in parameter_group:
        if p.parameter_name == marker or marker is None:
            marker = None
            if p.source == source:
                count += 1
                if count <= max_records:
                    parameters_local.append(p.describe())
                else:
                    marker = p.parameter_name
                    break

    result = {'Parameters': parameters_local}
    if marker is not None:
        result['Marker'] = marker

    return result


def modify_parameter_group(title, parameter_groups, parameter_group_name, parameters):
    """
    Modifies parameters in a parameter group.

    Args:
        title (str): The title key for the returned dictionary.
        parameter_groups (list): List of parameter group objects.
        parameter_group_name (str): The name of the parameter group to be modified.
        parameters (list): List of parameters to modify in the parameter group.

    Returns:
        dict: A dictionary with the title key and the modified parameter group name.

    Raises:
        ValueError: If the specified parameter group does not exist.
    """
    parameter_group = next(
        (p for p in parameter_groups if p.parameter_group_name == parameter_group_name), None)
    if parameter_group is None:
        raise ValueError(f"The parameter group does not exist")
    parameter_group.modify_parameters(parameters)
    return {title: parameter_group_name}


def delete_parameter_group(parameter_groups, parameter_group_name):
    """
    Function to delete a parameter group

    Args:
    parameter_groups (list): List of parameter groups to check against
    parameter_group_name (str): Name of the parameter group to delete

    Returns:
    bool: True if the deletion was successful, False otherwise

    Raises:
    ValueError: If the parameter group cannot be deleted or does not exist
    """
    if parameter_group_name == "default":
        raise ValueError(
            "You can't delete a default DB cluster parameter group")
    parameter_group_names = [p.parameter_group_name for p in parameter_groups]
    if parameter_group_name not in parameter_group_names:
        raise ValueError(f"The parameter group does not exist")
    for c in clusters:
        if c.parameter_group.parameter_group_name == parameter_group_name:
            raise ValueError(
                "This parameter group is in use and can't be deleted")
    parameter_group = [p for p in parameter_groups if
                       p.parameter_group_name == parameter_group_name]
    parameter_group.delete()
    parameter_groups.remove(parameter_group)


def create_db_cluster_parameter_group(db_cluster_parameter_group_name, db_parameter_group_family, description,
                                      tags=None):
    """
    Function to create a DB cluster parameter group

    Args:
    db_cluster_parameter_group_name (str): The name of the parameter group
    db_parameter_group_family (str): The family of the parameter group
    description (str): Description of the parameter group
    tags (list, optional): Additional tags (default: None)

    Returns:
    bool: True if the creation was successful, False otherwise

    Raises:
    ValueError: If the parameter group already exists
    """
    return create_parameter_group(db_cluster_parameter_groups, "db_cluster_parameter_group", "DBClusterParameterGroup",
                                  db_cluster_parameter_group_name, db_parameter_group_family, description, tags)


def delete_db_cluster_parameter_group(db_cluster_parameter_group_name):
    """
    Function to delete a DB cluster parameter group

    Args:
    db_cluster_parameter_group_name (str): The name of the parameter group to delete

    Returns:
    bool: True if the deletion was successful, False otherwise

    Raises:
    ValueError: If the parameter group cannot be deleted or does not exist
    """
    delete_parameter_group(db_cluster_parameter_groups,
                           db_cluster_parameter_group_name)


def describe_db_cluster_parameter_groups(db_cluster_parameter_group_name=None, max_records=100, marker=None):
    """
    Function to describe DB cluster parameter groups

    Args:
    db_cluster_parameter_group_name (str, optional): Name of the parameter group to search for (default: None)
    max_records (int, optional): Maximum number of records to return (default: 100)
    marker (str, optional): Marker to start the description from (default: None)

    Returns:
    dict: A dictionary describing the found parameter groups
    """
    return describe_parameter_groups(db_cluster_parameter_groups, 'DBClusterParameterGroups',
                                     db_cluster_parameter_group_name,
                                     max_records, marker)


def modify_db_cluster_parameter_group(db_cluster_parameter_group_name, parameters):
    """
    Modifies a DB cluster parameter group.

    Args:
        db_cluster_parameter_group_name (str): The name of the DB cluster parameter group to be modified.
        parameters (list): List of parameters to modify in the DB cluster parameter group.

    Returns:
        dict: A dictionary with the key 'DBClusterParameterGroupName' and the modified parameter group name.
    """
    return modify_parameter_group('DBClusterParameterGroupName', db_cluster_parameter_groups, db_cluster_parameter_group_name, parameters)


def describe_cluster_db_parameters(db_cluster_parameter_group_name, source='user', max_records=100, marker=None):
    """
    Retrieves a list of parameters for a specific DB cluster parameter group, filtered by source and limited by max_records.

    Args:
        db_cluster_parameter_group_name (str): The name of the DB cluster parameter group to retrieve parameters from.
        source (str, optional): The source of the parameters to return (default is 'user').
        max_records (int, optional): The maximum number of records to return in the response (default is 100).
        marker (str, optional): The marker to use for pagination. If None, starts from the beginning.

    Returns:
        dict: A dictionary containing:
            - 'Parameters': A list of parameters matching the criteria.
            - 'Marker' (optional): A marker indicating the position to resume pagination if there are more records.
    """
    return describe_parameters(db_cluster_parameter_groups, db_cluster_parameter_group_name, source, max_records, marker)


def create_db_parameter_group(db_parameter_group_family, db_parameter_group_name, description, tags=None):
    """
    Function to create a DB parameter group

    Args:
    db_parameter_group_family (str): The family of the parameter group
    db_parameter_group_name (str): The name of the parameter group
    description (str): Description of the parameter group
    tags (list, optional): Additional tags (default: None)

    Returns:
    bool: True if the creation was successful, False otherwise

    Raises:
    ValueError: If the parameter group already exists
    """
    return create_parameter_group(db_parameter_groups, "db_parameter_group", "DBParameterGroup",
                                  db_parameter_group_family,
                                  db_parameter_group_name, description, tags)


def delete_db_parameter_group(db_parameter_group_name):
    """
    Function to delete a DB parameter group

    Args:
    db_parameter_group_name (str): The name of the parameter group to delete

    Returns:
    bool: True if the deletion was successful, False otherwise

    Raises:
    ValueError: If the parameter group cannot be deleted or does not exist
    """
    delete_parameter_group(db_parameter_groups, db_parameter_group_name)


def describe_db_parameter_groups(db_parameter_group_name=None, max_records=100, marker=None):
    """
    Function to describe DB parameter groups

    Args:
    db_parameter_group_name (str, optional): Name of the parameter group to search for (default: None)
    max_records (int, optional): Maximum number of records to return (default: 100)
    marker (str, optional): Marker to start the description from (default: None)

    Returns:
    dict: A dictionary describing the found parameter groups
    """
    return describe_parameter_groups(db_parameter_groups, 'DBParameterGroups',
                                     db_parameter_group_name,
                                     max_records, marker)


def modify_db_parameter_group(db_parameter_group_name, parameters):
    """
    Modifies a DB parameter group.

    Args:
        db_parameter_group_name (str): The name of the DB parameter group to be modified.
        parameters (list): List of parameters to modify in the DB parameter group.

    Returns:
        dict: A dictionary with the key 'DBParameterGroupName' and the modified parameter group name.
    """
    return modify_parameter_group('DBParameterGroupName', db_parameter_groups, db_parameter_group_name, parameters)


def describe_db_parameters(db_parameter_group_name, source='user', max_records=100, marker=None):
    """
    Retrieves a list of parameters for a specific DB parameter group, filtered by source and limited by max_records.

    Args:
        db_parameter_group_name (str): The name of the DB parameter group to retrieve parameters from.
        source (str, optional): The source of the parameters to return (default is 'user').
        max_records (int, optional): The maximum number of records to return in the response (default is 100).
        marker (str, optional): The marker to use for pagination. If None, starts from the beginning.

    Returns:
        dict: A dictionary containing:
            - 'Parameters': A list of parameters matching the criteria.
            - 'Marker' (optional): A marker indicating the position to resume pagination if there are more records.
    """
    return describe_parameters(db_parameter_groups, db_parameter_group_name, source, max_records, marker)


def delete_db_cluster(clusters_in_func, cluster_identifier, delete_instances=True, backup=True, conn=None):
    """
    Function to delete a DB cluster

    Args:
    clusters_in_func (list): List of clusters to search within
    cluster_identifier (str): The ID of the cluster to delete
    delete_instances (bool): Whether to delete all instances of the cluster (default: True)
    backup (bool): Whether to perform a backup before deletion (default: True)

    Returns:
    bool: True if the deletion was successful, False otherwise

    Raises:
    ValueError: If the cluster is not found
    RuntimeError: If there is an error deleting the cluster
    """
    try:
        cluster = get_dbcluster_by_id(cluster_identifier, clusters_in_func)
        if backup:
            # Backup operation (not implemented here, just an example)
            print(f"Backing up cluster {cluster_identifier}")
        # if snapshot:
        #     if snapshot_id=='':
        #         raise ValueError("snapshot_id must have a valid value if a snapshot is created")
        #     is_valid_identifier(snapshot_id,255)
        #     # ביצוע גיבוי (לא ממומש כאן, רק דוגמה)
        #     print(f"Backing up cluster {cluster_id}")

        if delete_instances:
            cluster.delete_all_instances()
        else:
            cluster.make_instances_independent(conn)
        cluster.delete_all_endpoints()
        # cluster.delete_all_snapshots()
        delete_from_management("DBCluster", cluster_identifier, conn)
        clusters_in_func.remove(cluster)
        print(f"Cluster {cluster_identifier} deleted successfully.")
    except ValueError as e:
        raise ValueError(e)
    except Exception as e:
        raise RuntimeError(f"Error deleting cluster: {e}")


def is_valid_identifier(identifier, length):
    if not 1 <= len(identifier) <= length:
        raise ValueError(f"the length must be between 1 to {length}")

    # ביטוי רגולרי לבדיקת הדרישות הנוספות
    pattern = r'^[a-zA-Z][a-zA-Z0-9-]*[a-zA-Z0-9]$'
    if length == 63:
        pattern = r'^[a-zA-Z][a-zA-Z0-9-.]*[a-zA-Z0-9]$'

    # בדיקה שהמשתנה תואם את הביטוי הרגולרי ושאין בו שתי מקפים רצופים
    if not (re.match(pattern, identifier) and '--' not in identifier):
        raise ValueError("The identity syntax is incorrect")


def create_db_cluster_endpoint(cluster_identifier, endpoint_identifier, endpoint_type='READER', static_members=None,
                               excluded_members=None, conn=None, clusters_in_func=clusters):
    """
    Function to create a DB cluster endpoint

    Args:
    cluster_identifier (str): The ID of the cluster
    endpoint_identifier (str): The identifier of the endpoint
    endpoint_type (str): The type of the endpoint
    static_members (list): List of static members
    excluded_members (list, optional): List of excluded members (default: None)

    Returns:
    DBEndpoint: The created endpoint
    """
    endpoint_types = ['ANY', 'WRITER', 'READER']
    if not endpoint_type in endpoint_types:
        raise ValueError("the value of endpoint type is invalid")
    cluster = get_dbcluster_by_id(cluster_identifier, clusters_in_func)
    return cluster.add_endpoint(cluster_identifier, endpoint_identifier, endpoint_type, static_members,
                                excluded_members, conn)


def delete_db_cluster_endpoint(endpoint_identifier, clusters_in_func=clusters, conn=None):
    """
    Function to delete a DB cluster endpoint

    Args:
    endpoint_identifier (str): The identifier of the endpoint to delete

    Returns:
    bool: True if the deletion was successful, False otherwise
    """
    cluster, endpoint = find_cluster_and_endpoint_by_endpoint_identifier(
        endpoint_identifier, clusters_in_func)
    return cluster.delete_endpoint(endpoint, conn)


def describe_db_cluster_endpoints(endpoint_identifier, clusters_in_func=clusters):
    """
    Function to describe DB cluster endpoints

    Args:
    endpoint_identifier (str): The identifier of the endpoint to search for

    Returns:
    dict: Description of the found endpoint
    """
    _, endpoint = find_cluster_and_endpoint_by_endpoint_identifier(
        endpoint_identifier, clusters_in_func)
    return endpoint.describe()


def modify_db_cluster_endpoint(endpoint_identifier, endpoint_type='', static_members=None, excluded_members=None):
    """
    Modifies a DB cluster endpoint.

    Args:
        endpoint_identifier (str): The identifier of the endpoint to be modified.
        endpoint_type (str, optional): The type of the endpoint (e.g., 'READER', 'WRITER'). Default is ''.
        static_members (list, optional): List of static members to be associated with the endpoint. Default is None.
        excluded_members (list, optional): List of members to be excluded from the endpoint. Default is None.

    Returns:
        dict: The modified endpoint details.
    """
    cluster, endpoint = find_cluster_and_endpoint_by_endpoint_identifier(
        endpoint_identifier)
    return endpoint.modify(endpoint_type, static_members, excluded_members)
