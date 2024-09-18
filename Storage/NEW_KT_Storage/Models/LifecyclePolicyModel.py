import json
from datetime import datetime
from typing import List

from Storage.NEW_KT_Storage.DataAccess.ObjectManager import ObjectManager

class LifecyclePolicy:
    pk_column = 'policy_name'
    def __init__(self,
            policy_name: str,
            transitions_days_GLACIER: int,
            expiration_days:int,
            status: str = 'Enabled',
            prefix: List[str] =[],
            lifecycle_policy_id: str = None,
            creation_date: datetime = datetime.now().date(),

            ):
        """
        Initialize LifecyclePolicy attributes.

        :param policy_name: Name of the policy
        :param policy_id: Unique identifier for the policy
        :param status: Current status of the policy
        :param creation_date: Date the policy was created
        :param expiration_days: Date the policy will expire
        """
        self.policy_name = policy_name
        self.status = status
        self.prefix = prefix
        self.expiration_days =expiration_days
        self.transitions_days_GLACIER =transitions_days_GLACIER
        self.creation_date = creation_date
        self.lifecycle_policy_id = lifecycle_policy_id

    def __str__(self) -> str:
        """
        String representation of the LifecyclePolicy object.
        """
        policy_dict = self.to_dict()
        policy_str = "Lifecycle Policy Details:\n"
        policy_str += "\n".join([f"{key}: {value}" for key, value in policy_dict.items()])
        return policy_str
    def to_dict(self,db_path="C:\\Users\\User\\Desktop\\database\\Lifecycle.db") -> dict:
        """
        Convert LifecyclePolicy attributes to a dictionary.

        :return: Dictionary of the policy's attributes and values.
        """

        return ObjectManager.convert_object_attributes_to_dictionary(
            policy_name = self.policy_name,
            status = self.status,
            prefix=self.prefix,
            expiration_days = self.expiration_days,
            transitions_days_GLACIER = self.transitions_days_GLACIER,
            creation_date = self.creation_date,
        )

    def to_sql(self):
        data_dict = self.to_dict()
        values = '(' + ", ".join(f'\'{json.dumps(v)}\'' if isinstance(v, dict) or isinstance(v, list) else f'\'{v}\'' if isinstance(v, str) else f'\'{str(v)}\''
                           for v in data_dict.values()) + ')'
        return values

