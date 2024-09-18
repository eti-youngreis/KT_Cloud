from collections import defaultdict
from enum import Enum
import json
from typing import Dict, List, Tuple

from DB.NEW_KT_DB.DataAccess.ObjectManager import ObjectManager


class SourceType(Enum):
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
    RECOVERY = 'recovery'
    READ_REPLICA = 'read replica'
    FAILURE = 'failure'
    FAILOVER = 'failover'
    DELETION = 'deletion'
    CREATION = 'creation'
    CONFIGURATION_CHANGE = 'configuration change'
    BACKUP = 'backup'


class EventSubscription:

    pk_column = 'subscription_name'
    table_schema = """
        subscription_name TEXT PRIMARY KEY,
        source_type TEXT,
        source_ids TEXT,
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
        self.subscription_name = subscription_name
        self.source_type = source_type
        self.sources: Dict[SourceType, set] = defaultdict(set)

        for source_type, source_id in sources:
            self.sources[source_type.value].add(source_id)

        self.event_categories = event_categories
        self.sns_topic_arn = sns_topic_arn

        self.pk_value = self.subscription_name

    def to_dict(self) -> Dict:
        '''Retrieve the data of the event subscription as a dictionary.'''

        return ObjectManager.convert_object_attributes_to_dictionary(
            subscription_name=self.subscription_name,
            source_type=self.source_type.value,
            sources={source_type: list(sources)
                     for source_type, sources in self.sources.items()},
            event_categories=[
                event_category.value for event_category in self.event_categories],
            sns_topic_arn=self.sns_topic_arn
        )

    def to_sql(self) -> str:
        data = self.to_dict()
        values = []
        for _, value in data.items():
            if isinstance(value, dict):
                value = json.dumps(value)
            elif isinstance(value, list):
                value = json.dumps(value)
            values.append(f"'{value}'")
        return f"({', '.join(values)})"

    @staticmethod
    def get_object_name() -> str:
        return __class__.__name__.removesuffix('Model')

