from enum import Enum
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
    FIREHOSE_JSON = "FIREHOSE_JSON"
    FIREHOSE_RAW = "FIREHOSE_RAW"
    FIREHOSE_RAW_JSON = "FIREHOSE_RAW_JSON"
    FIREHOSE_RAW_JSON_STREAM = "FIREHOSE_RAW_JSON_STREAM"
    FIREHOSE_RAW_JSON_STREAM_JSON = "FIREHOSE_RAW_JSON_STREAM_JSON"
    FIREHOSE_RAW_JSON_STREAM_JSON_STREAM = "FIREHOSE_RAW_JSON_STREAM_JSON_STREAM"
    FIREHOSE_RAW_JSON_STREAM_JSON_STREAM_JSON = "FIREHOSE_RAW_JSON_STREAM_JSON_STREAM_JSON"


class SNSTopicModel:

    pk_column = 'topic_name'
    table_name = 'sns_topic'
    table_schema = 'topic_name text primary key not null, subscribers text'
    

    def __init__(self, topic_name: str):
        self.topic_name: str = topic_name
        self.subscribers: Dict[Protocol, List[str]] = {}
        self.pk_value = topic_name

    def to_dict(self):
        return ObjectManager.convert_object_attributes_to_dictionary(
            topic_name=self.topic_name,
            subscribers={protocol.value: [subscriber for subscriber in subscribers]
                         for protocol, subscribers in self.subscribers.items()}
        )

    
    def to_sql(self):
        return ObjectManager.convert_object_attributes_to_sql(
            topic_name=self.topic_name,
            subscribers={protocol.value: [subscriber for subscriber in subscribers]
                         for protocol, subscribers in self.subscribers.items()}
        )