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

    def __init__(self, topic_name: str, subscribers: Dict[Protocol, List[str]] = None):
        self.topic_name: str = topic_name
        if subscribers is None:
            self.subscribers = {}
        else:
            self.subscribers = subscribers
        self.pk_value = topic_name

    def to_dict(self):
        return {
            'topic_name': self.topic_name,
            'subscribers': {str(protocol): subscribers for protocol, subscribers in self.subscribers.items()}
        }

    def to_sql(self) -> str:
        '''Convert the object to a SQL-compatible string.'''
        data = self.to_dict()
        values = []
        for key, value in data.items():
            if isinstance(value, dict):
                # Convert Protocol enum keys to strings
                value = {str(k): v for k, v in value.items()}
                value = json.dumps(value)
            elif isinstance(value, list):
                value = json.dumps(value)
            values.append(f"'{value}'")  # Add quotes around each value for SQL
        return f"({', '.join(values)})"

    @staticmethod
    def get_object_name():
        return __class__.__name__.removesuffix('Model')
