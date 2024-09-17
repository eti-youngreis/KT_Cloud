import json
import os
import sys
import pytest

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from NEW_KT_DB.DataAccess.DBClusterManager import DBClusterManager
from NEW_KT_DB.Controller.DBClusterParameterGroupController import DBClusterParameterGroupController
from NEW_KT_DB.Service.Classes.DBClusterParameterGroupService import DBClusterParameterGroupService
from NEW_KT_DB.DataAccess.DBClusterParameterGroupManager import DBClusterParameterGroupManager
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../..')))
from Storage.NEW_KT_Storage.DataAccess.StorageManager import StorageManager

@pytest.fixture
def parameter_group_manager():
    return DBClusterParameterGroupManager(':memory:')

@pytest.fixture
def cluster_manager():
    return DBClusterManager(':memory:')

@pytest.fixture
def storage_manager():
    return StorageManager('test')

@pytest.fixture
def parameter_group_service(parameter_group_manager, cluster_manager, storage_manager):
    return DBClusterParameterGroupService(parameter_group_manager, cluster_manager, storage_manager)

@pytest.fixture
def parameter_group_controller(parameter_group_service):
    return DBClusterParameterGroupController(parameter_group_service)

# Generic function to create a parameter group
def create_parameter_group(controller, group_name, group_family, description):
    return controller.create_db_cluster_parameter_group(group_name, group_family, description)

# Generic function to check if a file exists
def assert_file_exists(file_name):
    assert os.path.exists(file_name), f"Expected file {file_name} was not created."

# Generic function to delete a file
def delete_file_if_exists(file_name):
    if os.path.exists(file_name):
        os.remove(file_name)

# Generic function to load JSON file and assert its content
def assert_json_content(file_name, expected_data):
    with open(file_name, 'r') as json_file:
        data = json.load(json_file)
        for key, value in expected_data.items():
            assert data[key] == value, f"Expected {key} to be {value}, but got {data[key]}"

# Generic function for file name
def generate_file_name_for_group (group_name):
    return f'db_cluster_parameter_groups/db_cluster_parameter_group_{group_name}.json'

def test_create_parameter_group(parameter_group_controller):
    group_name = "TestGroup"
    group_family = "TestFamily"
    description = "Test Description"
    file_name = generate_file_name_for_group(group_name)

    # Create the parameter group
    result = create_parameter_group(parameter_group_controller, group_name, group_family, description)
    assert result['DBClusterParameterGroupName'] == group_name
    assert result['DBParameterGroupFamily'] == group_family
    assert result['Description'] == description

    # Check if the correct file was created
    assert_file_exists(file_name)
    
    # Check if the file content matches the expected result
    expected_data = {'group_name': group_name, 'group_family': group_family, 'description': description}
    assert_json_content(file_name, expected_data)

    # Cleanup
    delete_file_if_exists(file_name)

def test_create_existing_parameter_group(parameter_group_controller):
    group_name = "TestGroup"
    group_family = "TestFamily"
    
    # Ensure the group exists
    create_parameter_group(parameter_group_controller, group_name, group_family, "Test Description")
    
    # Test if exception is raised when trying to create an existing group
    with pytest.raises(ValueError, match=f"ParameterGroup with NAME '{group_name}' already exists."):
        create_parameter_group(parameter_group_controller, group_name, group_family, "Another Description")

def test_create_parameter_group_with_invalid_name(parameter_group_controller):
    invalid_group_name = "InvalidGroupName!"

    # Test if exception is raised when trying to create a group with invalid_name
    with pytest.raises(ValueError, match=f"group_name {invalid_group_name} is not valid"):
        create_parameter_group(parameter_group_controller, invalid_group_name, "ValidFamily", "Valid Description")

def test_delete_parameter_group(parameter_group_controller):
    group_name = "TestGroup"
    file_name = generate_file_name_for_group(group_name)

    # Create the parameter group
    create_parameter_group(parameter_group_controller, group_name, "TestFamily", "Test Description")

    # Ensure the file exists before deletion
    assert_file_exists(file_name)

    # Delete the parameter group
    parameter_group_controller.delete_db_cluste_parameter_group(group_name)

    # Check if the file was deleted
    assert not os.path.exists(file_name), f"Expected file {file_name} was not deleted."

def test_delete_nonexistent_parameter_group(parameter_group_controller):
    group_name = "NonExistentGroup"

    # Test if exception is raised when trying to delete a non-existent group
    with pytest.raises(ValueError, match=f"Parameter Group '{group_name}' does not exist."):
        parameter_group_controller.delete_db_cluste_parameter_group(group_name)

def test_delete_parameter_group_with_associated_cluster(parameter_group_controller, cluster_manager):
    group_name = "TestGroup"
    file_name = generate_file_name_for_group(group_name)

    # Create the parameter group
    create_parameter_group(parameter_group_controller, group_name, "TestFamily", "Test Description")

    # Associate the parameter group with a cluster
    cluster_manager.create_cluster({"cluster_id": "TestCluster", "group_name": group_name})

    # Attempt to delete the parameter group, expect an exception due to association with cluster
    with pytest.raises(ValueError, match="Can't delete parameter group associated with any DB clusters"):
        parameter_group_controller.delete_db_cluste_parameter_group(group_name)

    # Cleanup
    delete_file_if_exists(file_name)

def test_delete_default_parameter_group(parameter_group_controller):
    group_name = "default"
    file_name = generate_file_name_for_group(group_name)

    # Create the default parameter group
    create_parameter_group(parameter_group_controller, group_name, "DefaultFamily", "Default group description")
    
    # Test if exception is raised when trying to delete the default group
    with pytest.raises(ValueError, match="You can't delete a default parameter group"):
        parameter_group_controller.delete_db_cluste_parameter_group(group_name)

    # Cleanup
    delete_file_if_exists(file_name)

def test_modify_parameter_group(parameter_group_controller):
    group_name = "TestGroup"
    file_name = generate_file_name_for_group(group_name)

    # Create a parameter group
    create_parameter_group(parameter_group_controller, group_name, "TestFamily", "Test Description")
    
    # Modify the parameter group with new parameters
    parameters = [
        {'ParameterName': 'backup_retention_period', 'ParameterValue': '14', 'IsModifiable': True, 'ApplyMethod': 'immediate'}
    ]
    parameter_group_controller.modify_db_cluste_parameter_group(group_name, parameters)

    # Check if the modifications were applied
    expected_parameters = {'parameters': [{'parameter_name': 'backup_retention_period', 'parameter_value': '14'}]}
    assert_json_content(file_name, expected_parameters)

    # Cleanup
    delete_file_if_exists(file_name)

def test_modify_nonexistent_parameter_group(parameter_group_controller):
    group_name = "NonExistentGroup"
    parameters = [{'ParameterName': 'backup_retention_period', 'ParameterValue': '14', 'IsModifiable': True, 'ApplyMethod': 'immediate'}]

    # Test if exception is raised when trying to modify a non-existent group
    with pytest.raises(KeyError, match=f"Parameter Group '{group_name}' does not exist."):
        parameter_group_controller.modify_db_cluste_parameter_group(group_name, parameters)

def test_modify_non_modifiable_parameter(parameter_group_controller):
    group_name = "TestGroup"
    file_name = generate_file_name_for_group(group_name)

    # Create the parameter group
    create_parameter_group(parameter_group_controller, group_name, "TestFamily", "Test Description")

    # Define a non-modifiable parameter
    parameters = [
        {'ParameterName': 'max_connections', 'ParameterValue': '100', 'IsModifiable': False, 'ApplyMethod': 'immediate'}
    ]
    parameter_group_controller.modify_db_cluste_parameter_group( group_name, parameters)

    # Attempt to change a non-modifiable parameter, expect an exception
    with pytest.raises(ValueError, match="You can't modify the parameter max_connections"):
        new_parameters = [
            {'ParameterName': 'max_connections', 'ParameterValue': '200', 'IsModifiable': False, 'ApplyMethod': 'immediate'}
        ]
        parameter_group_controller.modify_db_cluste_parameter_group(group_name, new_parameters)

    # Cleanup
    delete_file_if_exists(file_name)

def test_modify_with_invalid_is_modifiable(parameter_group_controller):
    group_name = "TestGroup"
    file_name = generate_file_name_for_group(group_name)

    # Create the parameter group
    create_parameter_group(parameter_group_controller, group_name, "TestFamily", "Test Description")


    
    invalid_parameters = [
        {'ParameterName': 'backup_retention_period', 'ParameterValue': '14', 'IsModifiable': 'invalid_value', 'ApplyMethod': 'immediate'}
    ]
    
    with pytest.raises(ValueError, match="value invalid_value is invalid for IsModifiable"):
        parameter_group_controller.modify_db_cluste_parameter_group(group_name, invalid_parameters)

    # Cleanup    
    delete_file_if_exists(file_name)    

def test_modify_with_invalid_apply_method(parameter_group_controller):
    group_name = "TestGroup"
    file_name = generate_file_name_for_group(group_name)
    
    create_parameter_group(parameter_group_controller, group_name, "TestFamily", "Test Description")

    
    invalid_parameters = [
        {'ParameterName': 'backup_retention_period', 'ParameterValue': '14', 'IsModifiable': True, 'ApplyMethod': 'invalid_value'}
    ]
    
    with pytest.raises(ValueError, match="value invalid_value is invalid for ApplyMethod"):
        parameter_group_controller.modify_db_cluste_parameter_group(group_name, invalid_parameters)

    # Cleanup    
    delete_file_if_exists(file_name) 

def test_describe_parameter_group(parameter_group_controller):
    group_name = "TestGroup"
    file_name = generate_file_name_for_group(group_name)

    # Create a parameter group
    create_parameter_group(parameter_group_controller, group_name, "TestFamily", "Test Description")

    # Describe the parameter group
    result = parameter_group_controller.describe_db_cluste_parameter_group(group_name)
    
    # Check the result contains the correct description
    assert result['Test Title'][0]['DBClusterParameterGroupName'] == group_name
    assert result['Test Title'][0]['DBParameterGroupFamily'] == "TestFamily"
    assert result['Test Title'][0]['Description'] == "Test Description"

    # Cleanup
    delete_file_if_exists(file_name)

def test_describe_nonexistent_parameter_group(parameter_group_controller):
    group_name = "NonExistentGroup"

    # Test if exception is raised when trying to describe a non-existent group
    with pytest.raises(KeyError, match=f"Parameter Group '{group_name}' does not exist."):
        parameter_group_controller.describe_db_cluste_parameter_group(group_name)
