from typing import Dict, Any
import json
import sqlite3
from DataAccess import ObjectManager

class AclObjectManager:
    def __init__(self, db_file: str):
        '''Initialize ObjectManager with the database connection.'''
        self.object_manager = ObjectManager(db_file)
        self.create_table()#???


    def createInMemoryAclObject(self):
        self.object_manager.save_in_memory()


    def deleteInMemoryAclObject(self):
        self.object_manager.delete_from_memory()


    def describeAclObject(self):
        self.object_manager.get_from_memory()


    def putAclObject(self):
        # add code to extract all data from self and send it as new updates
        updates = ''

        # self.object_manager.update_in_memory(updates)
        # '''Update an ACL object with new data.'''
        # # Create a dictionary of updates to send to the ObjectManager
        # criteria = f"id = '{acl_object_id}'"
        
        # # Prepare the updates to be sent, converting data to the appropriate format if necessary
        # self.object_manager.update_in_memory(updates, criteria)
















  from typing import Dict, List
from DataAccess import ObjectManager
from Models.ACLObjectModel import ACLObject

class ACLObjectsService:
    def __init__(self, db_file: str):
        '''Initialize ObjectManager with the database connection.'''
        self.object_manager = ObjectManager(db_file)
        self.create_table()

    def create_acl_object(self, owner: str, permissions: List[str]):
        '''Create a new ACL object and save it in memory.'''
        # יצירת אובייקט ACL חדש עם בעלים והרשאות
        new_acl_object = ACLObject(owner=owner, permissions=permissions)
        
        # שמירת אובייקט ה-ACL בזיכרון (או במסד הנתונים)
        self.object_manager.save_in_memory(new_acl_object)

    def delete_acl_object(self, acl_object_id: str):
        '''Delete an ACL object from memory using its unique ID.'''
        # מחיקת אובייקט ACL מהזיכרון לפי מזהה ייחודי (ID)
        criteria = f"id = '{acl_object_id}'"
        self.object_manager.delete_from_memory(criteria)

    def get_acl_object(self, acl_object_id: str) -> Dict:
        '''Retrieve the details of an ACL object from memory.'''
        # קבלת פרטי אובייקט ACL על פי ה-ID שלו
        return self.object_manager.get_object_from_memory(acl_object_id)

    def update_acl_object(self, acl_object_id: str, updates: Dict):
        '''Update the ACL object with new data.'''
        # עדכון אובייקט ACL בזיכרון עם ערכים חדשים
        criteria = f"id = '{acl_object_id}'"
        self.object_manager.update_in_memory(updates, criteria)

    def list_acl_objects(self) -> List[Dict]:
        '''List all ACL objects in memory.'''
        # שליפת כל אובייקטי ה-ACL מהזיכרון
        return self.object_manager.list_objects()

    def set_permissions(self, acl_object_id: str, permissions: List[str]):
        '''Update the permissions of a specific ACL object.'''
        # עדכון הרשאות עבור אובייקט ACL מסוים
        updates = {"permissions": permissions}
        self.update_acl_object(acl_object_id, updates)

    def get_permissions(self, acl_object_id: str) -> List[str]:
        '''Retrieve the permissions of a specific ACL object.'''
        acl_object = self.get_acl_object(acl_object_id)
        return acl_object.get('permissions', [])
  
