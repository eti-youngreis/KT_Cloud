from typing import Dict
from DataAccess import ObjectManager
import uuid
class Acl:

    TABLE_STRUCTURE =table_columns = "acl_id TEXT PRIMARY KEY ", "name TEXT  NOT NULL", "permissions TEXT  NOT NULL","bucket_id TEXT  NOT NULL","user_id TEXT  NOT NULL",

    def __init__(self, name,bucket_id ,user_id,permissions=[]): 

        # add relevant attributes in this syntax:
        # self.bucket_object_identifier = kwargs['bucket_object_identifier']
        # self.engine = kwargs['engine']
        self.acl_id = str(uuid.uuid1())
        self.name=name 
        self.permissions= permissions 
        self.user_id=user_id
        self.bucket_id=bucket_id
        self.pk_column = "acl_id"

    def to_dict(self) -> Dict:
        '''Retrieve the data of the DB cluster as a dictionary.'''

        # send relevant attributes in this syntax:
        return ObjectManager.convert_object_attributes_to_dictionary(
            acl_id=self.acl_id
            name= self.name,
            permission= self.Permission,
            user_id=self.user_id,
            bucket_id=self.bucket_id
            pk_column=self.pk_column,
            pk_value=self.pk_value
        )



    def to_sql(self) -> str:
            """Convert the AclObject instance to a SQL-friendly format."""
            data_dict = self.to_dict()
            try:
                values = (
                    "("
                    + ", ".join(
                        (
                            f"'{json.dumps(v)}'"
                            if isinstance(v, dict) or isinstance(v, list)
                            else f"'{v}'" if isinstance(v, str) else f"'{str(v)}'"
                        )
                        for v in data_dict.values()
                    )
                    + ")"
                )
                return values
            except Exception as e:
                print(f"Error converting to SQL format: {e}")
                return None

    