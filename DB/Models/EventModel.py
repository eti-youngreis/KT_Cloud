
from typing import Dict
from datetime import datetime
class EventModel:
    def __init__(self, enent_identifier:str, resource_identifier:str):
        self.event_identifier = enent_identifier
        self.resource_identifier = resource_identifier
        self.creation_date = datetime.now()

    def to_dict(self) -> Dict:
        return {
            'event_identifier':self.event_identifier,
            'resource_identifier':self.resource_identifier,
            'creation_date':self.creation_date
        }

