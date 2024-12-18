from collections import defaultdict
from enum import Enum
import json
from typing import Dict, List, Tuple

from traitlets import default

from DB.NEW_KT_DB.DataAccess.ObjectManager import ObjectManager


class SourceType(Enum):
    """
    Enumeration of possible source types for event subscriptions.
    """
    DB_INSTANCE = 'db-instance'
    DB_CLUSTER = 'db-cluster'
    DB_PARAMETER_GROUP = 'db-parameter-group'
    DB_SECURITY_GROUP = 'db-security-group'
    DB_SNAPSHOT = 'db-snapshot'
    DB_CLUSTER_SNAPSHOT = 'db-cluster-snapshot'
    DB_PROXY = 'db-proxy'
    ZERO_ETL = 'zero-etl'
    CUSTOM_ENGINE_VERSION = 'custom-engine-version'
    BLUE_GREEN_DEPLOYMENT = 'blue-green-deployment'
    ALL = 'all'


class EventCategory(Enum):
    """
    Enumeration of possible event categories for event subscriptions.
    """
    RECOVERY = 'recovery'
    READ_REPLICA = 'read replica'
    FAILURE = 'failure'
    FAILOVER = 'failover'
    DELETION = 'deletion'
    CREATION = 'creation'
    CONFIGURATION_CHANGE = 'configuration change'
    BACKUP = 'backup'


class EventSubscription:
    """
    Represents an event subscription in the database.
    """

    pk_column = 'subscription_name'
    table_structure = """
        subscription_name TEXT PRIMARY KEY,
        sources TEXT,
        source_type TEXT,
        event_categories TEXT,
        sns_topic TEXT"""

    def __init__(
        self,
        subscription_name: str,
        sources: List[Tuple[SourceType, str]],
        event_categories: List[EventCategory],
        sns_topic: str,
        source_type: SourceType
    ) -> None:
        """
        Initialize an EventSubscription object.

        Args:
            subscription_name (str): The name of the subscription.
            sources (List[Tuple[SourceType, str]]): List of source types and their IDs.
            event_categories (List[EventCategory]): List of event categories.
            sns_topic (str): The SNS topic to which notifications will be sent.
            source_type (SourceType): The type of source for which notifications will be received.
        """
        self.subscription_name = subscription_name
        self.source_type = source_type
        self.sources = defaultdict(set)

        for source_type, source_id in sources:
            self.sources[source_type.value].add(source_id)

        self.event_categories = event_categories
        self.sns_topic = sns_topic

        self.pk_value = self.subscription_name

    def __eq__(self, value: object) -> bool:
        """
        Compare two EventSubscription objects for equality.
        """
        if not isinstance(value, EventSubscription):
            return False
        return all([self.__getattribute__(attr) == value.__getattribute__(attr) for attr, _ in self.__dict__.items()]) and len(self.__dict__) == len(value.__dict__)

    def to_dict(self) -> Dict:
        """
        Convert the EventSubscription object to a dictionary.

        Returns:
            Dict: A dictionary representation of the EventSubscription.
        """
        return ObjectManager.convert_object_attributes_to_dictionary(
            subscription_name=self.subscription_name,
            sources={
                k: list(v) for k, v in self.sources.items()},
            source_type=self.source_type.value,
            event_categories=[
                ec.value for ec in self.event_categories],
            sns_topic=self.sns_topic)

    def to_sql(self) -> str:
        """
        Convert the EventSubscription object to an SQL insert statement.

        Returns:
            str: A string representation of the SQL insert statement.
        """
        data = self.to_dict()
        values = [
            f"'{data['subscription_name']}'",
            f"'{json.dumps(data['sources'])}'",
            f"'{data['source_type']}'",
            f"'{json.dumps(data['event_categories'])}'",
            f"'{data['sns_topic']}'"
        ]
        return f"({', '.join(values)})"

    @staticmethod
    def get_object_name() -> str:
        """
        Get the name of the object.

        Returns:
            str: The name of the object without the 'Model' suffix.
        """
        return __class__.__name__.removesuffix('Model')

    @staticmethod
    def values_to_dict(subscription_name, sources, source_type, event_categories, sns_topic) -> Dict:
        """
        Convert database values to a dictionary.

        Args:
            subscription_name (str): The name of the subscription.
            sources (str): JSON string of sources.
            source_type (str): The type of the source.
            event_categories (str): JSON string of event categories.
            sns_topic (str): The ARN of the SNS topic.

        Returns:
            Dict: A dictionary representation of the EventSubscription.
        """
        return {
            'subscription_name': subscription_name,
            'sources': json.loads(sources),
            'source_type': source_type,
            'event_categories': json.loads(event_categories),
            'sns_topic': sns_topic
        }
