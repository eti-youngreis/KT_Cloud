from enum import Enum
import json
from typing import Dict, List, Tuple

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
        sns_topic_arn TEXT"""

    def __init__(
        self,
        subscription_name: str,
        sources: List[Tuple[SourceType, str]],
        event_categories: List[EventCategory],
        sns_topic_arn: str,
        source_type: SourceType
    ) -> None:
        """
        Initialize an EventSubscription object.

        Args:
            subscription_name (str): The name of the subscription.
            sources (List[Tuple[SourceType, str]]): List of source types and their IDs.
            event_categories (List[EventCategory]): List of event categories.
            sns_topic_arn (str): The ARN of the SNS topic.
            source_type (SourceType): The type of the source.
        """
        self.subscription_name = subscription_name
        self.source_type = source_type
        self.sources = {source_type.value: set() for source_type in SourceType}

        for source_type, source_id in sources:
            self.sources[source_type.value].add(source_id)

        self.event_categories = event_categories
        self.sns_topic_arn = sns_topic_arn

        self.pk_value = self.subscription_name

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
            sns_topic_arn=self.sns_topic_arn)

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
            f"'{data['sns_topic_arn']}'"
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
    def values_to_dict(subscription_name, sources, source_type, event_categories, sns_topic_arn) -> Dict:
        """
        Convert database values to a dictionary.

        Args:
            subscription_name (str): The name of the subscription.
            sources (str): JSON string of sources.
            source_type (str): The type of the source.
            event_categories (str): JSON string of event categories.
            sns_topic_arn (str): The ARN of the SNS topic.

        Returns:
            Dict: A dictionary representation of the EventSubscription.
        """
        return {
            'subscription_name': subscription_name,
            'sources': json.loads(sources),
            'source_type': source_type,
            'event_categories': json.loads(event_categories),
            'sns_topic_arn': sns_topic_arn
        }
