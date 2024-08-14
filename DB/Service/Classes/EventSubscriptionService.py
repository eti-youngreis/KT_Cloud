from typing import List
from DataAccess import DataAccessLayer
from Models.EventSubscriptionModel import EventSubscriptionModel
from Models.SourceType import SourceType
from Service.Abc.DBO import DBO


class EventSubscriptionService(DBO):
    def __init__(self, dal: DataAccessLayer):
        self.dal = dal

    def create(self, subscription_name: str, sources: List[SourceType, str], event_categories: List[str],
               topic: str, source_type: SourceType):
        raise NotImplementedError

    def delete(self, subscription_name: str):
        raise NotImplementedError

    def describe(self, marker: str, max_records: int, subscription_name: str):
        raise NotImplementedError

    def modify(self, subscription_name: str, event_categories: List[str], topic: str, source_type: SourceType):
        raise NotImplementedError
