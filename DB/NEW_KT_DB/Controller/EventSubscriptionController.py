from typing import Any, Dict, List, Tuple
from DB.NEW_KT_DB.Models.EventSubscriptionModel import EventCategory, EventSubscription, SourceType
from DB.NEW_KT_DB.Service.Classes.EventSubscriptionService import EventSubscriptionService


class EventSubscriptionController:
    """
    Controller class for managing event subscriptions.
    """

    def __init__(self, service: EventSubscriptionService) -> None:
        """
        Initialize the EventSubscriptionController.

        Args:
            service (EventSubscriptionService): The service to handle event subscription operations.
        """
        self.service = service

    def create_event_subscription(self, subscription_name: str, sources: List[Tuple[SourceType, str]],
                                  event_categories: List[EventCategory], sns_topic: str, source_type: SourceType = SourceType.ALL) -> None:
        """
        Create a new event subscription.

        Args:
            subscription_name (str): The name of the subscription.
            sources (List[Tuple[SourceType, str]]): List of source types and their identifiers.
            event_categories (List[EventCategory]): List of event categories to subscribe to.
            sns_topic_arn (str): The ARN of the SNS topic for notifications.
            source_type (SourceType, optional): The type of source. Defaults to SourceType.All.
        """
        self.service.create(subscription_name=subscription_name, sources=sources,
                            event_categories=event_categories, sns_topic_arn=sns_topic, source_type=source_type)

    def delete_event_subscription(self, subscription_name: str):
        """
        Delete an event subscription.

        Args:
            subscription_name (str): The name of the subscription to delete.
        """
        self.service.delete(subscription_name=subscription_name)

    def describe_event_subscription(self, subscription_name: str) -> Dict:
        """
        Describe an event subscription.
        """
        return self.service.describe_by_id(subscription_name)

    def describe_event_subscriptions(self, columns: List[str] = '*', criteria: Dict[str, Any] = None) -> List[Dict]:
        """
        Describe event subscriptions.

        Args:
            subscription_name (str, optional): The name of a specific subscription to describe. Defaults to ''.
            columns (List[str], optional): List of columns to include in the description. Defaults to ['*'].
            criteria (Dict[str, Any], optional): Criteria for filtering subscriptions. Defaults to None.
        """
        return self.service.describe(columns, criteria)

    def modify_event_subscription(self, subscription_name: str, event_categories: List[EventCategory], sns_topic_arn: str, source_type: SourceType = SourceType.ALL) -> None:
        """
        Modify an existing event subscription.

        Args:
            subscription_name (str): The name of the subscription to modify.
            event_categories (List[EventCategory]): Updated list of event categories to subscribe to.
            sns_topic_arn (str): Updated ARN of the SNS topic for notifications.
            source_type (SourceType, optional): Updated type of source. Defaults to SourceType.ALL.
        """
        self.service.modify(subscription_name=subscription_name,
                            event_categories=event_categories, sns_topic_arn=sns_topic_arn, source_type=source_type)

    def get_event_subscriptions(self, criteria: Dict[str, Any] = None) -> List[EventSubscription]:
        """
        Get a list of all event subscriptions that match the given criteria.
        Args:
            criteria (Dict[str, Any], optional): Criteria for filtering subscriptions. Defaults to None.
        Returns:
            List[EventSubscription]: List of event subscriptions that match the criteria.
        """

        return self.service.get(criteria)

    def get_event_subscription(self, subscription_name: str) -> EventSubscription:
        """
        Retrieve a specific event subscription by its name.
        Args:
            subscription_name (str): The name of the subscription to retrieve.
        Returns:
            EventSubscription: The event subscription with the specified name.
        """

        return self.service.get_by_id(subscription_name)
