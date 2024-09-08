from typing import Dict, Any, List
import json
from DB.DataAccess.DBManager import DBManager


class EventSubscriptionManager:

    def __init__(self, db_file: str) -> None:
        """Initialize EventSubscriptionManager with the database connection."""
        self.db_manager = DBManager(db_file)
        self.table_name = 'event_subscription'

    def create(self, subscription_name: str, sources: List[str], event_categories: List[str], sns_topic_arn: str, source_type: str) -> None:
        """Create a new event subscription in the database."""
        metadata = {
            'sources': json.dumps(sources),
            'event_categories': json.dumps(event_categories),
            'sns_topic_arn': sns_topic_arn,
            'source_type': source_type
        }
        self.db_manager.insert(self.table_name, subscription_name, metadata)

    def update(self, subscription_name: str, sources: List[str] = None, event_categories: List[str] = None, sns_topic_arn: str = None, source_type: str = None) -> None:
        """Update an existing event subscription in the database."""
        updates = {}
        if sources is not None:
            updates['sources'] = json.dumps(sources)
        if event_categories is not None:
            updates['event_categories'] = json.dumps(event_categories)
        if sns_topic_arn is not None:
            updates['sns_topic_arn'] = sns_topic_arn
        if source_type is not None:
            updates['source_type'] = source_type

        if updates:
            self.db_manager.update(self.table_name, updates, f"subscription_name = {subscription_name}")

    def get(self, subscription_name: str) -> Dict[str, Any]:
        """Retrieve an event subscription from the database."""
        result = self.db_manager.select(self.table_name, [
                                        "subscription_name", "sources", "event_categories", "sns_topic_arn", "source_type"], f"subscription_name = '{subscription_name}'")
        if result:
            result[subscription_name]['sources'] = json.loads(
                result[subscription_name]['sources'])
            result[subscription_name]['event_categories'] = json.loads(
                result[subscription_name]['event_categories'])
            return result[subscription_name]
        else:
            raise FileNotFoundError(f'Subscription with name {subscription_name} not found.')

    def delete(self, subscription_name: str) -> None:
        """Delete an event subscription from the database."""
        self.db_manager.delete(self.table_name, f'subscription_name = {subscription_name}')

    def get_all_subscriptions(self) -> Dict[str, Dict[str, Any]]:
        """Retrieve all event subscriptions from the database."""
        results = self.db_manager.select(self.table_name, [
                                         "subscription_name", "sources", "event_categories", "sns_topic_arn", "source_type"])
        for key, value in results.items():
            value['sources'] = json.loads(value['sources'])
            value['event_categories'] = json.loads(value['event_categories'])
        return results

    def describe_table(self) -> Dict[str, str]:
        """Describe the schema of the table."""
        return self.db_manager.describe(self.table_name)

    def close(self) -> None:
        """Close the database connection."""
        self.db_manager.close()

    def get_sources(self, table_name: str, source_name: str) -> List[str]:
        """Retrieve source by name."""
        return self.db_manager.select(table_name, criteria=source_name)
