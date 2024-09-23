import sys
import os
import time
import pytest
from colorama import Fore, Style, init
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../../')))

# from NEW_KT_DB.DataAccess.DBClusterManager import DBClusterManager
# from NEW_KT_DB.Controller.DBClusterParameterGroupController import DBClusterParameterGroupController
# from NEW_KT_DB.Service.Classes.DBClusterParameterGroupService import DBClusterParameterGroupService
# from NEW_KT_DB.DataAccess.DBClusterParameterGroupManager import DBClusterParameterGroupManager
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../..')))
# from Storage.NEW_KT_Storage.DataAccess.StorageManager import StorageManager
from datetime import datetime
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..","..")))
from DB.NEW_KT_DB.Service.Classes.DBSnapshotNaiveService import DBSnapshotNaiveService
from DB.NEW_KT_DB.DataAccess.DBSnapshotNaiveManager import DBSnapshotNaiveManager
from DB.NEW_KT_DB.DataAccess.ObjectManager import ObjectManager


# init(autoreset=True)

# storage_manager = StorageManager('test')
# parameter_group_manager = DBClusterParameterGroupManager('INTEGRATION')
# cluster_manager= DBClusterManager('l')
# parameter_group_service = DBClusterParameterGroupService(parameter_group_manager, cluster_manager, storage_manager)
# parameter_group_controller = DBClusterParameterGroupController(parameter_group_service)


print('''---------------------Start Of session----------------------''')
start_time = datetime.now()
print(start_time)
print(f'''{datetime.now()} demonstration of DBSnapshot''')

# # create
# print('''___________________________________________________________''')
# print(Fore.YELLOW+f'''{datetime.now()} creating a DBClusterParameterGroup with name: \'parameter_group_1\''''+ Style.RESET_ALL)
# parameter_group_controller.create_db_cluster_parameter_group(
#        group_name='parameter_group_1',group_family='family', description='description'
#        )

# print("Creation of \'parameter_group_1\' passed with no exceptions")
# print(datetime.now()-start_time)

# time.sleep(10)
# # test create parameter_group
# print()
# test_name = 'test_create_parameter_group'
# print(f"Running test: {test_name}:")
# pytest.main(['-q', f'DB/NEW_KT_DB/Test/DBClusterParameterGroupTests.py::{test_name}'])

# time.sleep(3)
# # get
object_manager = ObjectManager('o_m')
dal = DBSnapshotNaiveManager(object_manager)
snapshot_service = DBSnapshotNaiveService(dal=dal)

start_time_get = datetime.now()
print('''___________________________________________________________''')
print(Fore.YELLOW+f'''{start_time} retrieving \'snapshot_1\''''+ Style.RESET_ALL)
parameter_group_1 = snapshot_service.get(db_snapshot_indentifier='snapshot_1')
print(parameter_group_1)
print(f'''{datetime.now()} retrieving \'snapshot_1\' passed with no exceptions, valid object returned''')
print(datetime.now() - start_time_get)

# # update
# time.sleep(8)
# start_time_update = datetime.now()
# print('''___________________________________________________________''')
# print(Fore.YELLOW+f'''{start_time} updating \'parameter_group_1\''''+ Style.RESET_ALL)
# updated_parameter_group = parameter_group_controller.modify_db_cluste_parameter_group(
#     group_name="parameter_group_1",
# parameters = [
#         {'ParameterName': 'backup_retention_period', 'ParameterValue': 14, 'IsModifiable': True, 'ApplyMethod': 'immediate'}
#     ]    
# )
# print(updated_parameter_group)

# print(f'''{datetime.now()} updating \'parameter_group_1\' passed with no exceptions''')
# print(datetime.now() - start_time_update)

# time.sleep(20)

# # test modify parameter_group
# print()
# test_name = 'test_modify_parameter_group'
# print(f"Running test: {test_name}:")
# pytest.main(['-q', f'DB/NEW_KT_DB/Test/DBClusterParameterGroupTests.py::{test_name}'])

# # delete
# start_time_delete = datetime.now()
# print('''___________________________________________________________''')
# print(Fore.YELLOW+f'''{start_time} deleting \'parameter_group_1\''''+ Style.RESET_ALL)
# parameter_group_controller.delete_db_cluste_parameter_group('parameter_group_1')

# print(f'''{datetime.now()} deleting \'parameter_group_1\' passed with no exceptions''')
# print(datetime.now() - start_time_delete)
# print('''___________________________________________________________''')

# time.sleep(10)

# # test delete parameter_group
# print()
# test_name = 'test_delete_parameter_group'
# print(f"Running test: {test_name}:")
# pytest.main(['-q', f'DB/NEW_KT_DB/Test/DBClusterParameterGroupTests.py::{test_name}'])

# # get

# start_time_get = datetime.now()
# print('''___________________________________________________________''')
# print(Fore.YELLOW+f'''{start_time} retrieving \'parameter_group_1\''''+ Style.RESET_ALL)
# try:
#     parameter_group_1 = parameter_group_service.get('parameter_group_1')
#     print(parameter_group_1)
# except Exception as e:   
#     print(e) 

# print(f'''{datetime.now()} demonstration of DBClusterParameterGroup ended successfully''')
# print(datetime.now())
# print('''---------------------End Of session----------------------''')
# print(datetime.now()-start_time)

