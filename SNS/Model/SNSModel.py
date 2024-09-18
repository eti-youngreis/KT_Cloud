from collections import defaultdict
from enum import Enum
import json
from typing import Dict, List

from DB.NEW_KT_DB.DataAccess.ObjectManager import ObjectManager


class Protocol(Enum):
    HTTP = "HTTP"
    HTTPS = "HTTPS"
    EMAIL = "EMAIL"
    EMAIL_JSON = "EMAIL_JSON"
    SMS = "SMS"
    SQS = "SQS"
    APPLICATION = "APPLICATION"
    LAMBDA = "LAMBDA"
    FIREHOSE = "FIREHOSE"


class SNSTopicModel:

    pk_column = 'topic_name'
    table_schema = 'topic_name TEXT PRIMARY KEY NOT NULL, subscribers TEXT'

    def __init__(self, topic_name: str):
        self.topic_name: str = topic_name
        self.subscribers: Dict[Protocol, List[str]] = defaultdict(list)
        self.pk_value = topic_name

    def to_dict(self):
        print(self.subscribers.keys())
        return ObjectManager.convert_object_attributes_to_dictionary(
            topic_name=self.topic_name,
            subscribers={protocol.value: [subscriber for subscriber in subscribers]
                         for protocol, subscribers in self.subscribers.items()}
        )

    def to_sql(self) -> str:
        '''Convert the object to a SQL-compatible string.'''
        data = self.to_dict()
        values = []
        for _, value in data.items():
            if isinstance(value, dict):
                value = json.dumps(value)  # Convert dict to JSON string
            elif isinstance(value, list):
                value = json.dumps(value)  # Convert list to JSON string
            values.append(f"'{value}'")  # Add quotes around each value for SQL
        return f"({', '.join(values)})"

    @staticmethod
    def get_object_name():
        return __class__.__name__.removesuffix('Model')
