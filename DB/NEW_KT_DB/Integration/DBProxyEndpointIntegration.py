from datetime import datetime
import os
import sys
import pytest
from unittest.mock import MagicMock
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))
from DB.NEW_KT_DB.Controller.DBProxyEndpointController import DBProxyEndpointController
from DB.NEW_KT_DB.Service.Classes.DBProxyEndpointService import DBProxyEndpointService
from DB.NEW_KT_DB.DataAccess.DBProxyEndpointManager import DBProxyEndpointManager
from DB.NEW_KT_DB.DataAccess.ObjectManager import ObjectManager
from Storage.NEW_KT_Storage.DataAccess.StorageManager import StorageManager
from DB.NEW_KT_DB.Exceptions.DBProxyEndpointExceptions import DBProxyEndpointNotFoundException

def db_proxy_service_mock():
    mock = MagicMock()
    mock.is_exists.side_effect = lambda db_proxy_name: db_proxy_name == 'my-proxy'
    return mock   

# Dependencies injection
object_manager = ObjectManager('DB/NEW_KT_DB/DBs/mainDB.db')
storage_manager = StorageManager("dbProxyEndpoints")
endpoint_manager = DBProxyEndpointManager(object_manager)
endpoint_service = DBProxyEndpointService(endpoint_manager, storage_manager, db_proxy_service_mock())
endpoint_controller = DBProxyEndpointController(endpoint_service)

DB_PROXY_ENDPOINT_NAME = "endpoint"
NEW_DB_PROXY_ENDPOINT_NAME = "our-endpoint"
DB_PROXY_NAME = "my-proxy"
VPC_SUBNET_IDS = ['subnet-12345678', 'subnet-87654321']
TARGET_ROLE = "READ_WRITE"

# Demonstrate all dbproxy endpoint functionallity

print('''---------------------Start Of session----------------------''')
print()


print(f'''{datetime.now()} deonstration of object db proxy endpoint start''')
print()
print("----------------------------------------------------------------")

# create
start_time = datetime.now()
print(f'''{start_time} going to create db proxy endpoint names "endpoint" in db proxy "my proxy"''')
res = endpoint_controller.create_db_proxy_endpoint(DB_PROXY_NAME, DB_PROXY_ENDPOINT_NAME, VPC_SUBNET_IDS, TARGET_ROLE)
end_time = datetime.now()
print(f'''{end_time} db proxy endpoint "endpoint" created successfully''')
duration = end_time - start_time
print("duration: ",duration)
print("----------------------------------------------------------------")

# Test create
print("-----Test create---------")
print("Response: ",res)
print(endpoint_controller.describe_db_proxy_endpoint(DB_PROXY_ENDPOINT_NAME))
pytest.main(['-q', 'DB/NEW_KT_DB/Test/test_DBProxyEndpointTests.py::test_create_db_proxy_endpoint'])
pytest.main(['-q', 'DB/NEW_KT_DB/Test/test_DBProxyEndpointTests.py::test_create_when_db_proxy_not_exist'])
pytest.main(['-q', 'DB/NEW_KT_DB/Test/test_DBProxyEndpointTests.py::test_create_db_proxy_endpoint_with_existing_name'])
print("----------------------------------------------------------------")

# Delete
start_time = datetime.now()
print(f'''{start_time} going to delete db db proxy endpoint "endpoint"''')
res = endpoint_controller.delete_db_proxy_endpoint(DB_PROXY_ENDPOINT_NAME)
end_time = datetime.now()
print(f'''{end_time} db proxy endpoint "endpoint" deleted successfully''')
duration = end_time - start_time
print("duration: ",duration)
print("----------------------------------------------------------------")

# Test delete
print("-----Test delete---------")
try:
    print(endpoint_controller.describe_db_proxy_endpoint(DB_PROXY_ENDPOINT_NAME))
except DBProxyEndpointNotFoundException as e:
    print(e)
pytest.main(['-q', 'DB/NEW_KT_DB/Test/test_DBProxyEndpointTests.py::test_delete_db_proxy_endpoint'])
pytest.main(['-q', 'DB/NEW_KT_DB/Test/test_DBProxyEndpointTests.py::test_delete_non_valid_state_db_proxy_endpoint'])
print("----------------------------------------------------------------")


# Modify
endpoint_before_modify = endpoint_controller.create_db_proxy_endpoint(DB_PROXY_NAME, DB_PROXY_ENDPOINT_NAME, VPC_SUBNET_IDS, TARGET_ROLE)
start_time = datetime.now()
print(f'''{start_time} going to modify db proxy endpoint "endpoint" to name "our-endpoint"''')
endpoint_after_modify = endpoint_controller.modify_db_proxy_endpoint(DB_PROXY_ENDPOINT_NAME, NEW_DB_PROXY_ENDPOINT_NAME)
end_time = datetime.now()
print(f'''{end_time} db db proxy endpoint "endpoint" modified successfully''')
duration = end_time - start_time
print("duration: ",duration)
print("----------------------------------------------------------------")

# Test modify
print("Test modify")
print("before modify:")
print(endpoint_before_modify)
print("after modify:")
print(endpoint_after_modify)
pytest.main(['-q', 'DB/NEW_KT_DB/Test/test_DBProxyEndpointTests.py::test_modify_name_to_db_proxy_endpoint'])
pytest.main(['-q', 'DB/NEW_KT_DB/Test/test_DBProxyEndpointTests.py::test_modify_non_exist_db_proxy_endpoint'])

# Describe
start_time = datetime.now()
print(f'''{start_time} going to describe db proxy endpoint "our-endpoint"''')
print(endpoint_controller.describe_db_proxy_endpoint(NEW_DB_PROXY_ENDPOINT_NAME))
end_time = datetime.now()
duration = end_time - start_time
print(f'''{end_time} db proxy endpoint "our-endpoint" was described successfully''')
print("duration: ",duration)
print("----------------------------------------------------------------")

# Test describe
print("Test describe")
pytest.main(['-q', 'DB/NEW_KT_DB/Test/test_DBProxyEndpointTests.py::test_describe_db_proxy_endpoint'])
pytest.main(['-q', 'DB/NEW_KT_DB/Test/test_DBProxyEndpointTests.py::test_describe_with_filters'])
pytest.main(['-q', 'DB/NEW_KT_DB/Test/test_DBProxyEndpointTests.py::test_describe_with_non_correct_filters'])

print(f'''{datetime.now()} deonstration of object db proxy endpoint ended successfully''')
print('''---------------------End Of session----------------------''')

# delete from memory all integration leftovers
endpoint_controller.delete_db_proxy_endpoint(NEW_DB_PROXY_ENDPOINT_NAME)



