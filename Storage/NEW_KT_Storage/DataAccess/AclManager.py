from Models.AclModel import Acl
from DB.NEW_KT_DB.DataAccess.ObjectManager import ObjectManager as ObjectManagerDB

class AclManager:
    def __init__(self, db_file: "str"):
        '''Initialize ObjectManager with the database connection.'''
        self.object_manager = ObjectManagerDB(db_file)
        self.object_name = "Acl"
        self.object_manager.object_manager.create_management_table('mng_Acl', AclModel.TABLE_STRUCTURE)

   


    def get_acl_object_from_memory(self, acl_id: str):
        return self.object_manager.get_from_memory(
                self.object_name, 
                criteria=f"{Acl.pk_column}='{acl_id}'"
        )
            
    



    def createInMemoryAcl(self,acl:Acl):
        """Save a TagObject instance in memory."""
        return self.object_manager.save_in_memory(
            self.object_name, acl.to_sql()
        )
    
    def deleteInMemoryAcl(self,acl_id:str):
        self.object_manager.delete_from_memory_by_pk(
            self.object_name, Acl.pk_column, acl_id
        )


    def describeAcl(self):
        self.object_manager.get_from_memory(self.object_name)



    def putAcl(self,updates: str = None):
        # add code to extract all data from self and send it as new updates
        if not updates:
            raise ValueError("No fields to update")


        self.object_manager.update_in_memory(
            self.object_name,
            updates,
            f""" {Acl.pk_column}='{Acl.pk_value}' """,
        )
        


