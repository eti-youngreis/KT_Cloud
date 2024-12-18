import json
import os
import sys
import pytest
from unittest.mock import Mock, patch
from unittest.mock import MagicMock
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from NEW_KT_DB.DataAccess.DBClusterManager import DBClusterManager
from NEW_KT_DB.Controller.DBClusterParameterGroupController import DBClusterParameterGroupController
from NEW_KT_DB.Service.Classes.DBClusterParameterGroupService import DBClusterParameterGroupService
from NEW_KT_DB.DataAccess.DBClusterParameterGroupManager import DBClusterParameterGroupManager
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../..')))
from Storage.NEW_KT_Storage.DataAccess.StorageManager import StorageManager
from GeneralTests import *

# Generic function for file name
def generate_file_name_for_group (group_name):
    return f'db_cluster_parameter_groups/db_cluster_parameter_group_{group_name}.json'

group_name = "TestGroup"
group_family = "TestFamily"
description = "Test Description"
file_name = generate_file_name_for_group(group_name)

@pytest.fixture
def parameter_group_manager():
    return DBClusterParameterGroupManager('t')
# :memory:
@pytest.fixture
def cluster_manager():
    # Create a mock for DBClusterManager and its method get_all_clusters
    mock_cluster_manager = Mock(spec=DBClusterManager)
    # Set the return value of get_all_clusters
    mock_cluster_manager.get_all_clusters.return_value = {}
    return mock_cluster_manager

@pytest.fixture
def parameter_group_service(parameter_group_manager, cluster_manager, storage_manager):
    return DBClusterParameterGroupService(parameter_group_manager, cluster_manager, storage_manager)

@pytest.fixture
def parameter_group_controller(parameter_group_service):
    return DBClusterParameterGroupController(parameter_group_service)

# Generic function to create a parameter group
def create_parameter_group(controller, group_name, group_family, description):
    return controller.create_db_cluster_parameter_group(group_name, group_family, description)

# Generic function to assert the parameter group's details 
def assert_parameter_group_details(result, index, expected_group_name, expected_family, expected_description):
    """
    Assert the details of a specific DBClusterParameterGroup in the result.

    :param result: The result dictionary returned from the describe_db_cluste_parameter_group function.
    :param index: The index of the parameter group in the result list to check.
    :param expected_group_name: The expected DBClusterParameterGroupName value.
    :param expected_family: The expected DBParameterGroupFamily value.
    :param expected_description: The expected Description value.
    """
    parameter_group = result['DBClusterParameterGroup'][index]
    
    assert parameter_group['DBClusterParameterGroupName'] == expected_group_name, \
        f"Expected DBClusterParameterGroupName to be '{expected_group_name}' but got '{parameter_group['DBClusterParameterGroupName']}'"
    assert parameter_group['DBParameterGroupFamily'] == expected_family, \
        f"Expected DBParameterGroupFamily to be '{expected_family}' but got '{parameter_group['DBParameterGroupFamily']}'"
    assert parameter_group['Description'] == expected_description, \
        f"Expected Description to be '{expected_description}' but got '{parameter_group['Description']}'"

def test_create_parameter_group(parameter_group_controller, storage_manager):
    # group_name1=group_name+'1'
    # Create the parameter group
    result = create_parameter_group(parameter_group_controller, group_name, group_family, description)
    assert result['DBClusterParameterGroupName'] == group_name
    assert result['DBParameterGroupFamily'] == group_family
    assert result['Description'] == description
    full_path = os.path.abspath(file_name)
    print(f"Full path of the file: {full_path}")
    # Check if the correct file was created
    assert_file_exist(storage_manager, file_name)
    
    # Check if the file content matches the expected result
    expected_data = {'group_name': group_name, 'group_family': group_family, 'description': description}
    assert_json_content(storage_manager, file_name, expected_data)

    # Cleanup
    storage_manager.delete_file(file_name)

def test_create_existing_parameter_group(parameter_group_controller):
    group_name0=group_name+'0'
       # Ensure the group exists
    create_parameter_group(parameter_group_controller, group_name0, group_family, "Test Description")
    
    # Test if exception is raised when trying to create an existing group
    with pytest.raises(ValueError, match=f"ParameterGroup with NAME '{group_name}' already exists."):
        create_parameter_group(parameter_group_controller, group_name, group_family, "Another Description")

def test_create_parameter_group_with_invalid_name(parameter_group_controller):
    invalid_group_name = "InvalidGroupName!"

    # Test if exception is raised when trying to create a group with invalid_name
    with pytest.raises(ValueError, match=f"group_name {invalid_group_name} is not valid"):
        create_parameter_group(parameter_group_controller, invalid_group_name, "ValidFamily", "Valid Description")

def test_delete_parameter_group(parameter_group_controller, storage_manager):
    group_name1=group_name+'1'
    file_name = generate_file_name_for_group(group_name1)

    # Create the parameter group
    create_parameter_group(parameter_group_controller, group_name1, "TestFamily", "Test Description")

    # Ensure the file exists before deletion
    assert_file_exist(storage_manager, file_name)

    # Delete the parameter group
    parameter_group_controller.delete_db_cluste_parameter_group(group_name1)

    # Check if the file was deleted
    assert not os.path.exists(file_name), f"Expected file {file_name} was not deleted."

def test_delete_parameter_group_with_associated_cluster(parameter_group_controller, storage_manager):
    group_name2=group_name+'2'
    file_name = generate_file_name_for_group(group_name2)

     # Create the parameter group
    create_parameter_group(parameter_group_controller, group_name2, "TestFamily", "Test Description")

    # Mock get_all_clusters to return a cluster associated with the parameter group
    parameter_group_controller.service.dal_cluster.get_all_clusters.return_value =[("","","","","","",group_name2)] #{"TestCluster": {"group_name": group_name}}

    # Attempt to delete the parameter group, expect an exception due to association with cluster
    with pytest.raises(ValueError, match="Can't delete parameter group associated with any DB clusters"):
        parameter_group_controller.delete_db_cluste_parameter_group(group_name2)

    # Cleanup
    storage_manager.delete_file(file_name)

def test_delete_nonexistent_parameter_group(parameter_group_controller):
    group_name = "NonExistentGroup"

    # Test if exception is raised when trying to delete a non-existent group
    with pytest.raises(ValueError, match=f"Parameter Group '{group_name}' does not exist."):
        parameter_group_controller.delete_db_cluste_parameter_group(group_name)

def test_delete_default_parameter_group(parameter_group_controller, storage_manager):
    group_name = "default"
    file_name = generate_file_name_for_group(group_name)
    # Create the default parameter group
    create_parameter_group(parameter_group_controller, group_name, "DefaultFamily", "Default group description")
    
    # Test if exception is raised when trying to delete the default group
    with pytest.raises(ValueError, match="You can't delete a default parameter group"):
        parameter_group_controller.delete_db_cluste_parameter_group(group_name)

    # Cleanup
    storage_manager.delete_file(file_name)

def test_modify_parameter_group(parameter_group_controller, storage_manager):
    group_name3=group_name+'3'
    file_name = generate_file_name_for_group(group_name3)

    # Create a parameter group
    create_parameter_group(parameter_group_controller, group_name3, group_family, description)
    
    # Modify the parameter group with new parameters
    parameters = [
        {'ParameterName': 'backup_retention_period', 'ParameterValue': 14, 'IsModifiable': True, 'ApplyMethod': 'immediate'}
    ]
    parameter_group_controller.modify_db_cluste_parameter_group(group_name3, parameters)

    # Check if the modifications were applied
    expected_parameters = {'parameters': [{'parameter_name': 'backup_retention_period', 'parameter_value': 14, 'description': '',
     'is_modifiable': True, 'apply_method': 'immediate'}, {'parameter_name': 'preferred_backup_window', 'parameter_value': '03:00-03:30',
      'description': '', 'is_modifiable': True, 'apply_method': ''}, {'parameter_name': 'preferred_maintenance_window',
       'parameter_value': 'Mon:00:00-Mon:00:30', 'description': '', 'is_modifiable': True, 'apply_method': ''}]}
  
    assert_json_content(storage_manager, file_name, expected_parameters)

    # Cleanup
    storage_manager.delete_file(file_name)

def test_modify_nonexistent_parameter_group(parameter_group_controller):
    group_name = "NonExistentGroup"
    parameters = [{'ParameterName': 'backup_retention_period', 'ParameterValue': 14, 'IsModifiable': True, 'ApplyMethod': 'immediate'}]

    # Test if exception is raised when trying to modify a non-existent group
    with pytest.raises(ValueError, match=f"Parameter Group '{group_name}' does not exist."):
        parameter_group_controller.modify_db_cluste_parameter_group(group_name, parameters)

def test_modify_non_modifiable_parameter(parameter_group_controller, storage_manager):
    group_name4=group_name+'4'
    file_name = generate_file_name_for_group(group_name4)

       # Create the parameter group
    create_parameter_group(parameter_group_controller, group_name4, group_family, description)

    # Define a non-modifiable parameter
    parameters = [
        {'ParameterName': 'backup_retention_period', 'ParameterValue': 5, 'IsModifiable': False, 'ApplyMethod': 'immediate'}
    ]
    parameter_group_controller.modify_db_cluste_parameter_group( group_name4, parameters)

    # Attempt to change a non-modifiable parameter, expect an exception
    with pytest.raises(ValueError, match="You can't modify the parameter backup_retention_period"):
        new_parameters = [
            {'ParameterName': 'backup_retention_period', 'ParameterValue': 14, 'IsModifiable': False, 'ApplyMethod': 'immediate'}
        ]
        parameter_group_controller.modify_db_cluste_parameter_group(group_name4, new_parameters)

    # Cleanup
    storage_manager.delete_file(file_name)

def test_modify_with_invalid_is_modifiable(parameter_group_controller, storage_manager):
    group_name5=group_name+'5'
    file_name = generate_file_name_for_group(group_name5)

    # Create the parameter group
    create_parameter_group(parameter_group_controller, group_name5, group_family, description)


    
    invalid_parameters = [
        {'ParameterName': 'backup_retention_period', 'ParameterValue': '14', 'IsModifiable': 'invalid_value', 'ApplyMethod': 'immediate'}
    ]
    
    with pytest.raises(ValueError, match="value invalid_value is invalid for IsModifiable"):
        parameter_group_controller.modify_db_cluste_parameter_group(group_name5, invalid_parameters)

    # Cleanup    
    storage_manager.delete_file(file_name)

def test_modify_with_invalid_apply_method(parameter_group_controller, storage_manager):
    group_name6=group_name+'6'
    file_name = generate_file_name_for_group(group_name6)

    
    create_parameter_group(parameter_group_controller, group_name6, group_family, description)

    
    invalid_parameters = [
        {'ParameterName': 'backup_retention_period', 'ParameterValue': '14', 'IsModifiable': True, 'ApplyMethod': 'invalid_value'}
    ]
    
    with pytest.raises(ValueError, match="value invalid_value is invalid for ApplyMethod"):
        parameter_group_controller.modify_db_cluste_parameter_group(group_name6, invalid_parameters)

    # Cleanup    
    storage_manager.delete_file(file_name)

def test_describe_parameter_group(parameter_group_controller, storage_manager):
    group_name7=group_name+'7'
    file_name = generate_file_name_for_group(group_name7)

    # Create a parameter group
    create_parameter_group(parameter_group_controller, group_name7, group_family, description)
 
    # Describe the parameter group
    result = parameter_group_controller.describe_db_cluste_parameter_group(group_name7)
    # Check the result contains the correct description
    assert_parameter_group_details(result, 0, group_name7, group_family, description)
    result = parameter_group_controller.describe_db_cluste_parameter_group()
    # Check the result contains the correct description
    assert_parameter_group_details(result, 8, group_name7, group_family, description)


    # Cleanup
    storage_manager.delete_file(file_name)

def test_describe_nonexistent_parameter_group(parameter_group_controller):
    group_name = "NonExistentGroup"

    # Test if exception is raised when trying to describe a non-existent group
    with pytest.raises(ValueError, match=f"Parameter Group '{group_name}' does not exist."):
        parameter_group_controller.describe_db_cluste_parameter_group(group_name)

def test_describe_group_without_parameter_group_name(parameter_group_controller, storage_manager):
    max_records = 2
    marker = None


    # Mock the return of get_all_groups method to simulate multiple parameter groups
    mock_parameter_groups = {
        "Group1": {"group_name": "Group1", "family": "TestFamily1", "description": "Description 1"},
        "Group2": {"group_name": "Group2", "family": "TestFamily2", "description": "Description 2"},
        "Group3": {"group_name": "Group3", "family": "TestFamily3", "description": "Description 3"},
    }
    for p in mock_parameter_groups.values():
        create_parameter_group(parameter_group_controller, p['group_name'], p['family'], p['description'])
    # Call the describe_group without a parameter_group_name
    result = parameter_group_controller.describe_db_cluste_parameter_group(max_records=max_records, marker=marker)

    # Check that the correct number of parameter groups are returned based on max_records
    assert len(result["DBClusterParameterGroup"]) == max_records
    # Check if pagination marker is returned
    assert 'Marker' in result