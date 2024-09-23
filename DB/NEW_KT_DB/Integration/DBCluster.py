import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../..')))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from KT_Cloud.Storage.NEW_KT_Storage.DataAccess import StorageManager
from Service.Classes.DBClusterService import DBClusterService
from DataAccess import DBClusterManager
from Controller.DBInstanceNaiveController import DBInstanceController
from Controller.DBClusterController import DBClusterController
from Service.Classes.DBInstanceNaiveService import DBInstanceService
from DataAccess.DBInstanceNaiveManager import DBInstanceManager
from DataAccess.ObjectManager import ObjectManager
from datetime import datetime
import time
from colorama import init, Fore, Back, Style
init()

print(Fore.GREEN + '''---------------------Start Of session----------------------''')
start_current_time = datetime.now()
formatted_time = start_current_time.strftime("%Y-%m-%d %H:%M:%S")
print(Fore.GREEN +f'''{formatted_time} demonstration of object Cluster start''')

db_instance_controller=DBInstanceController(DBInstanceService(DBInstanceManager(ObjectManager("DBs/mainDB.db"))))

storage_manager = StorageManager.StorageManager('DBs/Cluster')
clusterManager = DBClusterManager.DBClusterManager('DBs/mainDB.db')
clusterService = DBClusterService(clusterManager,storage_manager, './')
clusterController = DBClusterController(clusterService,db_instance_controller)

print(Fore.BLUE + "\n---------------------Create DBCluster----------------------")
start_date_time = datetime.now()
print(Fore.RED + f'''{start_date_time} going to create db cluster names "example"''')
cluster_data = {
    'db_cluster_identifier': 'example',
    'engine': 'mysql',
    'allocated_storage':5,
    # 'db_subnet_group_name': 'my-subnet-group'
    }
clusterController.create_db_cluster(**cluster_data)
end_date_time = datetime.now()
print(Fore.RED + f'''{end_date_time} cluster "example" created successfully''')
print(f"Total creation time: {end_date_time - start_date_time}")
time.sleep(4)

print(Fore.BLUE + "\n---------------------Describe DBCluster----------------------")
start_date_time = datetime.now()
print(Fore.RED + f'''{start_date_time} going to describe db cluster names "example"''')
description = clusterController.describe_db_cluster('example')
print(Fore.WHITE +f"\nDB cluster 'example' description: {description}\n")
end_date_time = datetime.now()
print(Fore.RED +f"{end_date_time} DBCluster 'example' described successfully")
print(f"Total description time: {end_date_time - start_date_time}")
time.sleep(4)

print(Fore.BLUE + "\n---------------------Modify DBCluster----------------------")
start_date_time = datetime.now()
print(Fore.RED + f'''{start_date_time} going to modify db cluster names "example"''')
update_data = {
    'engine': 'postgres',
    'allocated_storage':3,
    }
clusterController.modify_db_cluster('example', **update_data)
end_date_time = datetime.now()
print(Fore.RED +f"{end_date_time} DBCluster 'example' modified successfully")
print(f"Total modification time: {end_date_time - start_date_time}")
time.sleep(4)

print(Fore.BLUE + "\n---------------------Describe Modified DBCluster----------------------")
start_date_time = datetime.now()
print(Fore.RED + f'''{start_date_time} going to describe db cluster names "example"''')
description = clusterController.describe_db_cluster('example')
highlight_words = ['postgres',3]
# Loop through the items in the tuple and color the selected words
print(Fore.WHITE +f"\nDB cluster 'example' description: [(", end="")
for item in description[0]:
    if item in highlight_words:
        print(Fore.GREEN + str(item) + Style.RESET_ALL, end=", ")  # Green for highlighted words
    else:
        print(Fore.WHITE + str(item) + Style.RESET_ALL, end=", ")
print(Fore.WHITE +")]\n")

end_date_time = datetime.now()
print(Fore.RED +f"{end_date_time} DBCluster 'example' described successfully")
print(f"Total description time: {end_date_time - start_date_time}")
time.sleep(4)

print(Fore.BLUE + "\n---------------------Delete DBCluster----------------------")
start_date_time = datetime.now()
print(Fore.RED + f'''{start_date_time} going to delete db cluster names "example"''')
clusterController.delete_db_cluster('example')
end_date_time = datetime.now()
print(Fore.RED +f"{end_date_time} DBCluster 'example' deleted successfully")
print(f"Total deletion time: {end_date_time - start_date_time}")
time.sleep(4)

print(Fore.BLUE + "\n---------------------Attempt to Describe Deleted DBCluster----------------------")
start_date_time = datetime.now()
print(Fore.RED + f'''{start_date_time} going to attempt description of deleted db cluster names "example"\n''')
try:
    description = clusterController.describe_db_cluster('example')
    print("Error: DBCluster 'example' still exists after deletion\n")
except Exception as e:
    print(Fore.YELLOW +f"Success: Unable to get deleted DBCluster 'example'. Error: {str(e)}\n")

end_date_time = datetime.now()
print(Fore.RED +f"{end_date_time} DBCluster 'example' described successfully")
print(f"Total description time: {end_date_time - start_date_time}\n")
time.sleep(4)

end_current_time = datetime.now()
end_formatted_time = end_current_time.strftime("%Y-%m-%d %H:%M:%S")
print(Fore.GREEN + '''---------------------End Of session----------------------''')
print(Fore.GREEN +f'''{end_formatted_time} demonstration of object Cluster ended successfully''')
print(f"Total demonstration time: {end_current_time - start_current_time}")

