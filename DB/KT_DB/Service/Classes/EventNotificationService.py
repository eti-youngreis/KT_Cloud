from typing import List
from DB.KT_DB.DataAccess.EventSubscriptionManager import EventSubscriptionManager
from DB.KT_DB.DataAccess.EventManager import EventManager
from DB.KT_DB.Models.EventModel import EventModel
from DB.KT_DB.Models.EventSubscriptionModel import EventSubscriptionModel


class EventNotificationService:
    def __init__(self, event_manager: EventManager, subscription_manager: EventSubscriptionManager):
        self.event_manager = event_manager
        self.subscription_manager = subscription_manager

    def process_pending_events(self):
        pending_events = self.event_manager.get_all_unprocessed_events()
        for event in pending_events:
            event_id = event['event_id']
            self._process_event(event_id=event_id)
            self.event_manager.update(
                event_id=event_id, metadata=event, processed=True)

    def _process_event(self, event: EventModel) -> None:
        subscriptions: List[EventSubscriptionModel] = self.subscription_manager.get_all_subscriptions(
        )
        for subscription in subscriptions:
            if self._event_matches_subscription(event, subscription):
                self._send_notification(event, subscription)

    def _event_matches_subscription(self, event: EventModel, subscription: EventSubscriptionModel) -> bool:
        return (event.event_identifier in subscription.event_identifiers
                and event.resource_identifier in subscription.resource_identifiers)

    def _send_notification(self, event, subscription):
        # Implement notification sending logic here
        pass
