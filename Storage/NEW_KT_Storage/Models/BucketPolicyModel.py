from datetime import datetime
from typing import Dict, Optional
from DataAccess import ObjectManager

class BucketPolicy:

    def __init__(self, 
            bucket_name: str, 
            policy_content: str, 
            policy_name: str, 
            version: str = "2012-10-17", 
            tags: Optional[Dict] = None, 
            available: bool = True, 
            pk_column: str = '', 
            pk_value: Optional[str] = None):
        """
        Initializes the BucketPolicy object with explicit parameters.
        """
        self.bucket_name = bucket_name
        self.policy_content = policy_content
        self.policy_name = policy_name
        self.version = version
        self.tags = tags if tags else {}
        self.available = available
    
        # attributes for memory management in database
        self.pk_column = pk_column
        self.pk_value = pk_value


def to_dict(self) -> Dict:
    '''Retrieve the data of the DB cluster as a dictionary.'''

    return ObjectManager.convert_object_attributes_to_dictionary(
        
        bucket_name=self.bucket_name,
        policy_content=self.policy_content,
        policy_name=self.policy_name,
        version=self.version,
        tags=self.tags if self.tags else {},
        available=self.available,
        
        pk_column=self.pk_column,
        pk_value=self.pk_value
    )
