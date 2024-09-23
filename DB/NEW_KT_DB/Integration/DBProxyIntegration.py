
from DataAccess.ObjectManager import ObjectManager
from Storage.NEW_KT_Storage.DataAccess.StorageManager import StorageManager
from DB.NEW_KT_DB.DataAccess.DBManager import EmptyResultsetError
from DB.NEW_KT_DB.Controller.DBProxyController import DBProxyController
from DB.NEW_KT_DB.DataAccess.ObjectManager import ObjectManager
from DB.NEW_KT_DB.DataAccess.DBProxyManager import DBProxyManager
from Storage.NEW_KT_Storage.DataAccess.StorageManager import StorageManager
from DB.NEW_KT_DB.Service.Classes.DBProxyService import DBProxyService
from datetime import datetime

object_manager = ObjectManager('../DBs/mainDB.db')
storage_manager = StorageManager('../../s3')
proxy_manager = DBProxyManager(object_manager)
proxy_service = DBProxyService(proxy_manager, storage_manager)
proxy_controller = DBProxyController(proxy_service)

print("---------------------Start Of session----------------------")
start_integration_date_time = datetime.now()
