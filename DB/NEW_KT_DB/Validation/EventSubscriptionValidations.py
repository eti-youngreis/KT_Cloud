from DB.NEW_KT_DB.DataAccess.EventSubscriptionManager import EventSubscriptionManager


def validate_subscription_name_exist(dal: EventSubscriptionManager, subscription_name: str):
    try:
        dal.describeEventSubscriptionById(subscription_name)
        return True
    except:
        raise ValueError(f'subscription {subscription_name} does not exist')
