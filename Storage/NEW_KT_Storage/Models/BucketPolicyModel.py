from datetime import datetime
import json
from typing import Dict, Optional
import uuid

class BucketPolicy:
    """
    A class to represent a bucket policy, which defines permissions and
    versioning for a bucket.
    
    Attributes:
    -----------
    pk_column : str
        The primary key column name for the database.
    object_name : str
        The object name, used for identifying the class type.
    table_structure : str
        SQL table structure for storing bucket policy information.
    policy_id : str
        Unique identifier for the bucket policy.
    bucket_name : str
        Name of the bucket to which this policy applies.
    permissions : list
        A list of permissions associated with the bucket.
    allow_versions : bool
        Boolean indicating whether versioning is allowed for the bucket.
    """

    pk_column = 'policy_id'
    object_name = 'BucketPolicy'
    table_structure = '''
                policy_id VARCHAR(255) PRIMARY KEY NOT NULL,
                bucket_name VARCHAR(255) NOT NULL,
                permissions TEXT NOT NULL,
                allow_versions BOOLEAN NOT NULL
            '''

    def __init__(self, bucket_name: str, permissions: list, allow_versions=True):
        """
        Initializes a BucketPolicy instance with the given bucket name,
        permissions, and versioning option.

        Parameters:
        -----------
        bucket_name : str
            The name of the bucket.
        permissions : list
            A list of permissions for the bucket (e.g., read, write).
        allow_versions : bool, optional
            Specifies whether versioning is allowed (default is True).
        """
        self.policy_id = self._generate_policy_id(bucket_name)
        self.bucket_name = bucket_name
        self.permissions = permissions
        self.allow_versions = allow_versions

    def to_dict(self) -> Dict:
        """
        Converts the bucket policy instance to a dictionary representation.

        Returns:
        --------
        Dict
            A dictionary with all the policy attributes.
        """
        return self.convert_object_attributes_to_dictionary(
            policy_id=self.policy_id,
            bucket_name=self.bucket_name,
            permissions=self.permissions,
            allow_versions=self.allow_versions
        )
    
    def convert_object_attributes_to_dictionary(self, **kwargs) -> Dict:
        """
        Converts the provided keyword arguments to a dictionary.

        Parameters:
        -----------
        **kwargs : dict
            Arbitrary keyword arguments representing policy attributes.
        
        Returns:
        --------
        dict
            A dictionary containing the provided keyword arguments.
        """
        dict_obj = {}
        for key, value in kwargs.items():
            dict_obj[key] = value
        return dict_obj

    def _generate_policy_id(self, bucket_name: str) -> str:
        """
        Generates a unique policy ID by concatenating the bucket name with a UUID.

        Parameters:
        -----------
        bucket_name : str
            The name of the bucket to generate a policy ID for.
        
        Returns:
        --------
        str
            A unique policy ID for the bucket.
        """
        unique_id = str(uuid.uuid4())
        policy_id = f"{bucket_name}_{unique_id}"
        return policy_id

    def to_sql(self) -> str:
        """
        Converts the bucket policy instance into an SQL-compatible values string.

        Returns:
        --------
        str
            A string representing the policy data in SQL values format.
        """
        data_dict = self.to_dict()
        values = '(' + ", ".join(
            f'\'{json.dumps(v)}\'' if isinstance(v, dict) or isinstance(v, list) else f'\'{v}\'' if isinstance(v, str) else f'\'{str(v)}\''
            for v in data_dict.values()
        ) + ')'
        return values
