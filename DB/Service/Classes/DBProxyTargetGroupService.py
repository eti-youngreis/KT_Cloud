from Abc import DBO
from typing import Dict, Optional, List
from Models.DBProxyModel import DBProxyModel
from DataAccess.ObjectManager import ObjectManager
from Validation.Validation import check_filters_validation
from exceptions.exception import ParamValidationError
from exceptions.db_proxy_exception import *

class DBProxyTargetGroupService(DBO):
    def __init__(self, dal:ObjectManager):
        self.db_proxies = {}
        self.dal = dal
        self.object_type = 'DBProxy'
    
    def create():
        pass
    
    def delete():
        pass
    
    def modify():
        pass
    
    def describe():
        pass

    
        
