
from DB.NEW_KT_DB.Controller.DBProxyController import DBProxyController
from DB.NEW_KT_DB.DataAccess.ObjectManager import ObjectManager
from DB.NEW_KT_DB.DataAccess.DBProxyManager import DBProxyManager
from Storage.NEW_KT_Storage.DataAccess.StorageManager import StorageManager
from DB.NEW_KT_DB.Service.Classes.DBProxyService import DBProxyService
from datetime import datetime
import time

object_manager = ObjectManager('../DBs/mainDB.db')
storage_manager = StorageManager('../s3/proxies')
proxy_manager = DBProxyManager(object_manager)
proxy_service = DBProxyService(proxy_manager, storage_manager)
proxy_controller = DBProxyController(proxy_service)
time_to_sleep = 1

print("---------------------Start Of session----------------------")
start_integration_date_time = datetime.now()
print(f"{start_integration_date_time} demonstration of object DBProxy start")

print("\n-------------------Create DBProxy-------------------------")
start_creation_date_time = datetime.now()
print(f"{start_creation_date_time} going to create DBProxy names 'proxy-example'")
proxy_controller.create_db_proxy(
    db_proxy_name='proxy-example',
    engine_family='MYSQL',
    role_arn='arn:aws:iam::account-id:role/my-iam-role',
    auth={
        'AuthScheme': 'SECRETS',
        'SecretArn': 'arn:aws:secretsmanager:region:account-id:secret:mysecret',
        'IAMAuth': 'DISABLED'
    },
    vpc_subnet_ids=['subnet-12345678', 'subnet-87654321'],)
end_creation_date_time = datetime.now()
total_duration = end_creation_date_time - start_creation_date_time
print(f'''{end_creation_date_time} DBProxy 'proxy-example' created successfully''')
print(f"Total creation time: {total_duration}")
time.sleep(time_to_sleep)


print("\n-------------------Describe DBProxy----------------------")
start_description_date_time = datetime.now()
print(f"{start_description_date_time} going to describe db DBProxy 'proxy-example'")
description = proxy_controller.describe_db_proxy(db_proxy_name='proxy-example')
print(f"\nDBProxy 'proxy-example' description: {description}\n")
end_description_date_time = datetime.now()
print(f"{end_description_date_time} DBProxy 'proxy-example' described successfully!")
print(f"Total description time: {end_description_date_time - start_description_date_time}")
time.sleep(time_to_sleep)


print("\n---------------------Modify DBProxy----------------------")
start_modification_date_time = datetime.now()
print(f"{start_modification_date_time} going to modify db DBProxy 'proxy-example'")
proxy_controller.modify_db_proxy(db_proxy_name='proxy-example',engine_family='POSTGRESQL',)
end_modification_date_time = datetime.now()
print(f"{end_modification_date_time} DBProxy 'proxy-example' modified successfully!")
print(f"Total modification time: {end_modification_date_time - start_modification_date_time}")
time.sleep(time_to_sleep)


print("\n---------------------Describe Modified DBProxy----------------------")
start_description_date_time = datetime.now()
print(f"{start_description_date_time} going to describe db DBProxy 'proxy-example' after modification")
description = proxy_controller.describe_db_proxy(db_proxy_name='proxy-example')
print(f"\nDBProxy 'proxy-example' description after modification: {description}\n")
end_description_date_time = datetime.now()
print(f"{end_description_date_time} DBProxy 'proxy-example' described successfully")
print(f"Total description time: {end_description_date_time - start_description_date_time}")
time.sleep(time_to_sleep)


print("\n---------------------Get DBProxy----------------------")
start_get_date_time = datetime.now()
print(f"{start_get_date_time} going to get db DBProxy 'proxy-example'")
proxy_controller.get(db_proxy_name='proxy-example')
end_get_date_time = datetime.now()
print(f"{end_get_date_time} DBProxy 'proxy-example' get successfully")
print(f"Total get time: {end_get_date_time - start_get_date_time}")
time.sleep(time_to_sleep)


print("\n---------------------Delete DBProxy----------------------")
start_deletion_date_time = datetime.now()
print(f"{start_deletion_date_time} going to delete db DBProxy 'proxy-example'")
proxy_controller.delete_db_proxy(db_proxy_name='proxy-example')
end_deletion_date_time = datetime.now()
print(f"{end_deletion_date_time} DBProxy 'proxy-example' deleted successfully")
print(f"Total deletion time: {end_deletion_date_time - start_deletion_date_time}")
time.sleep(time_to_sleep)


print("\n---------------------End Of session----------------------")
end_integration_date_time = datetime.now()
print(f"{end_integration_date_time} demonstration of object DBProxy ended successfully")
print(f"Total integration time: {end_integration_date_time - start_integration_date_time}")
time.sleep(time_to_sleep)
