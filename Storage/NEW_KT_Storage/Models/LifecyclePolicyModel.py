import json
from datetime import datetime
from typing import List
from Storage.NEW_KT_Storage.DataAccess.ObjectManager import ObjectManager

class LifecyclePolicy:
    pk_column = 'policy_name'
    table_structure = ", ".join([
        "policy_name TEXT PRIMARY KEY",
        "bucket_name TEXT",
        "status TEXT",
        "prefix TEXT",
        "expiration_days INTEGER",
        "transitions_days_glacier INTEGER",
        "creation_date DATETIME"
    ])

    def __init__(self,
                 policy_name: str,
                 bucket_name: str,
                 expiration_days: int,
                 transitions_days_glacier: int,
                 status: str = 'Enabled',
                 prefix: List[str] = None,
                 lifecycle_policy_id: str = None,
                 creation_date: datetime = None):
        self.policy_name = policy_name
        self.bucket_name = bucket_name
        self.status = status
        self.prefix = prefix or []
        self.expiration_days = expiration_days
        self.transitions_days_glacier = transitions_days_glacier
        self.creation_date = creation_date or datetime.now().date()
        self.lifecycle_policy_id = lifecycle_policy_id

    def __str__(self) -> str:
        policy_details = self.to_dict()
        return "Lifecycle Policy Details:\n" + "\n".join([f"{key}: {value}" for key, value in policy_details.items()])

    def to_dict(self) -> dict:
        return ObjectManager.convert_object_attributes_to_dictionary(
            policy_name=self.policy_name,
            bucket_name=self.bucket_name,
            status=self.status,
            prefix=self.prefix,
            expiration_days=self.expiration_days,
            transitions_days_glacier=self.transitions_days_glacier,
            creation_date=self.creation_date,
        )

    def to_sql(self) -> str:
        data_dict = self.to_dict()
        values = '(' + ', '.join(
            f"'{json.dumps(v)}'" if isinstance(v, (dict, list)) else f"'{v}'" if isinstance(v, str) else f"'{str(v)}'"
            for v in data_dict.values()
        ) + ')'
        return values

