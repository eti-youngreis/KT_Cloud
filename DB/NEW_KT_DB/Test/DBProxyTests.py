import sys
import os
import sqlite3
import json
import pytest
from DB.NEW_KT_DB.DataAccess.DBManager import EmptyResultsetError
from DB.NEW_KT_DB.Controller.DBProxyController import DBProxyController
from DB.NEW_KT_DB.DataAccess.ObjectManager import ObjectManager
from DB.NEW_KT_DB.DataAccess.DBProxyManager import DBProxyManager
from Storage.NEW_KT_Storage.DataAccess.StorageManager import StorageManager
from DB.NEW_KT_DB.Service.Classes.DBProxyService import DBProxyService
from DB.NEW_KT_DB.Exceptions.DBProxyExceptions import DBProxyAlreadyExistsFault, DBProxyNotFoundFault

# Adding path for the modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))

# Defining file paths
db_path = 'object_management_db.db'  # Path to the database
files_path = 'DB\\Proxies\\'  # Path to proxy files

# Creating instances of the object managers and services
object_manager = ObjectManager(db_path)
storage_manager = StorageManager(files_path)
proxy_manager = DBProxyManager(object_manager)
proxy_service = DBProxyService(proxy_manager, storage_manager)
proxy_controller = DBProxyController(proxy_service)

# Defining a sample proxy name
db_proxy_name = 'example-db-proxy'


@pytest.fixture
def cleanup_db_and_files():
    """
    Fixture for cleaning up the database and files after each test.
    Deletes all records from the table and removes files related to the proxy.
    """
    yield  # Allows tests to run before cleanup
    conn = sqlite3.connect(db_path)  # Connect to the database
    cursor = conn.cursor()
    table_name = "mng_DBProxys"
    cursor.execute(f"DELETE FROM {table_name};")  # Delete all records from the table
    conn.commit()  # Save changes
    conn.close()  # Close the connection
    storage_manager.delete_file(db_proxy_name + '.json')  # Delete JSON file


@pytest.fixture
def create_db_proxy():
    """
    Fixture for creating a new proxy before each test.
    Creates a sample proxy with fixed parameters.
    """
    proxy_controller.create_db_proxy(
        db_proxy_name='example-db-proxy',
        engine_family='MYSQL',
        role_arn='arn:aws:iam::account-id:role/my-iam-role',
        auth={
            'AuthScheme': 'SECRETS',
            'SecretArn': 'arn:aws:secretsmanager:region:account-id:secret:mysecret',
            'IAMAuth': 'DISABLED'
        },
        vpc_subnet_ids=['subnet-12345678', 'subnet-87654321'],
    )


def _assert_proxy_does_not_exist(proxy_name):
    """
    Assert to check if the proxy does not exist in the system.

    Parameters:
    proxy_name (str): The name of the proxy to check.
    """
    assert not storage_manager.is_file_exist(proxy_name)  # Check if the file does not exist
    assert not proxy_manager.is_db_proxy_exist(proxy_name)  # Check if the proxy does not exist


def test_create_db_proxy(cleanup_db_and_files):
    """
    Test for creating a new proxy.

    Ensures that the proxy is created successfully and that a JSON file is generated.
    """
    proxy_controller.create_db_proxy(
        db_proxy_name=db_proxy_name,
        engine_family='MYSQL',
        role_arn='arn:aws:iam::account-id:role/my-iam-role',
        auth={
            'AuthScheme': 'SECRETS',
            'SecretArn': 'arn:aws:secretsmanager:region:account-id:secret:mysecret',
            'IAMAuth': 'DISABLED'
        },
        vpc_subnet_ids=['subnet-12345678', 'subnet-87654321'],
    )
    assert storage_manager.is_file_exist(db_proxy_name + '.json')  # Check if the file exists
    assert proxy_manager.is_db_proxy_exist(db_proxy_name)  # Check if the proxy exists


def test_create_db_proxy_when_db_proxy_name_already_exists(create_db_proxy, cleanup_db_and_files):
    """
    Test for creating a proxy with an existing name.

    Ensures that the method raises an error if the name already exists.
    """
    with pytest.raises(DBProxyAlreadyExistsFault):
        proxy_controller.create_db_proxy(
            db_proxy_name=db_proxy_name,
            engine_family='MYSQL',
            role_arn='arn:aws:iam::account-id:role/my-iam-role',
            auth={
                'AuthScheme': 'SECRETS',
                'SecretArn': 'arn:aws:secretsmanager:region:account-id:secret:mysecret',
                'IAMAuth': 'DISABLED'
            },
            vpc_subnet_ids=['subnet-12345678', 'subnet-87654321'],
        )


def test_create_db_proxy_when_db_proxy_name_not_valid(cleanup_db_and_files):
    """
    Test for creating a proxy with an invalid name.

    Ensures that the method raises an error for an invalid name.
    """
    not_valid_proxy_name = 'not--valid'
    with pytest.raises(ValueError,
                       match="Invalid DB Proxy name. Must start with a letter, contain only ASCII letters, digits, and hyphens, and cannot end with a hyphen"):
        proxy_controller.create_db_proxy(
            db_proxy_name=not_valid_proxy_name,
            engine_family='MYSQL',
            role_arn='arn:aws:iam::account-id:role/my-iam-role',
            auth={
                'AuthScheme': 'SECRETS',
                'SecretArn': 'arn:aws:secretsmanager:region:account-id:secret:mysecret',
                'IAMAuth': 'DISABLED'
            },
            vpc_subnet_ids=['subnet-12345678', 'subnet-87654321'],
        )
    _assert_proxy_does_not_exist(not_valid_proxy_name)  # Check that the proxy does not exist


def test_create_db_proxy_when_a_required_parameter_missing(cleanup_db_and_files):
    """
    Test for creating a proxy when a required parameter is missing.

    Ensures that the method raises an error when there is an invalid parameter.
    """
    with pytest.raises(TypeError, match=r"There is an invalid parameter in the input"):
        proxy_controller.create_db_proxy(
            db_proxy_name=db_proxy_name,
            engine_family='MYSQL',
            role_arn='arn:aws:iam::account-id:role/my-iam-role',
            auth={
                'AuthScheme': 'SECRETS',
                'SecretArn': 'arn:aws:secretsmanager:region:account-id:secret:mysecret',
                'IAMAuth': 'DISABLED'
            },
            vpc_subnet_ids=['subnet-12345678', 'subnet-87654321'],
            invalid_parameter=123  # Invalid parameter
        )
    _assert_proxy_does_not_exist(db_proxy_name)  # Check that the proxy does not exist


def test_create_db_proxy_when_an_invalid_parameter_exists_there(cleanup_db_and_files):
    """
    Test for creating a proxy when there is an invalid parameter.

    Ensures that the method raises an error when a required parameter is missing.
    """
    with pytest.raises(TypeError, match=r"Missing required parameter in input"):
        proxy_controller.create_db_proxy(
            db_proxy_name=db_proxy_name,
            engine_family='MYSQL',
            role_arn='arn:aws:iam::account-id:role/my-iam-role',
            auth={
                'AuthScheme': 'SECRETS',
                'SecretArn': 'arn:aws:secretsmanager:region:account-id:secret:mysecret',
                'IAMAuth': 'DISABLED'
            }
        )
    _assert_proxy_does_not_exist(db_proxy_name)  # Check that the proxy does not exist


def test_delete_db_proxy(create_db_proxy, cleanup_db_and_files):
    """
    Test for deleting an existing proxy.

    Ensures that the proxy is deleted successfully.
    """
    proxy_controller.delete_db_proxy(db_proxy_name)
    _assert_proxy_does_not_exist(db_proxy_name)  # Check that the proxy does not exist


def test_delete_db_proxy_when_db_proxy_not_exist(cleanup_db_and_files):
    """
    Test for deleting a proxy that does not exist.

    Ensures that the method raises an error when the proxy does not exist.
    """
    with pytest.raises(DBProxyNotFoundFault):
        proxy_controller.delete_db_proxy(db_proxy_name)


def test_modify_db_proxy(create_db_proxy, cleanup_db_and_files):
    """
    Test for modifying an existing proxy.

    Ensures that the changes are saved in the JSON file.
    """
    role_arn = 'new_arn:aws:iam::account-id:role/my-iam-role'
    proxy_controller.modify_db_proxy(db_proxy_name=db_proxy_name, role_arn=role_arn)
    content_of_file = storage_manager.get_file_content(db_proxy_name + '.json')
    dict_content = json.loads(content_of_file)  # Convert file content to a dictionary
    assert dict_content['role_arn'] == role_arn  # Check if the role_arn is updated
    assert dict_content['create_date'] != dict_content['update_date']  # Check if the date has changed


def test_modify_db_proxy_when_db_proxy_not_exist(cleanup_db_and_files):
    """
    Test for modifying a proxy that does not exist.

    Ensures that the method raises an error when the proxy does not exist.
    """
    role_arn = 'new_arn:aws:iam::account-id:role/my-iam-role'
    with pytest.raises(DBProxyNotFoundFault):
        proxy_controller.modify_db_proxy(db_proxy_name=db_proxy_name, role_arn=role_arn)


def test_describe_db_proxy(create_db_proxy, cleanup_db_and_files):
    """
    Test for retrieving the description of an existing proxy.

    Ensures that the returned object is valid.
    """
    expected_length = 14  # Expected number of values in the tuple
    proxy = proxy_controller.describe_db_proxy(db_proxy_name)
    assert proxy is not None  # Check if the result is not empty
    assert isinstance(proxy, tuple)  # Check if the result is a tuple
    assert len(proxy) == expected_length  # Check if the number of values in the tuple is correct
    assert proxy[0] == db_proxy_name  # Check if the proxy name matches


def test_describe_db_proxy_when_db_proxy_not_exist(cleanup_db_and_files):
    """
    Test for retrieving the description of a proxy that does not exist.

    Ensures that the method raises an error when the proxy does not exist.
    """
    with pytest.raises(DBProxyNotFoundFault):
        proxy_controller.describe_db_proxy(db_proxy_name)
