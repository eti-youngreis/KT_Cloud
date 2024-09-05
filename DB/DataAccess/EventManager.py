from typing import Dict, Any
import json
from DBManager import DBManager

class EventManager:
    def __init__(self, db_file: str):
        self.db_manager = DBManager(db_file)
        self.table_name = 'events'
        self.create_table()
    
    def create_table(self):
        table_schema = 'event_id INTEGER PRIMARY KEY AUTOINCREMENT, event_type TEXT NOT NULL, metadata TEXT NOT NULL'
        self.db_manager.create_table(self.table_name, table_schema)
        
    def create(self, event_type: str, metadata: Dict[str, Any]) -> None:
        self.db_manager.insert(self.table_name, event_type, json.dumps(metadata))

    def update(self, event_id: int, metadata: Dict[str, Any]) -> None:
        self.db_manager.update(self.table_name, {'metadata': json.dumps(metadata)}, f'event_id = {event_id}')

    def get(self, event_id: int) -> Dict[str, Any]:
        result = self.db_manager.select(self.table_name, ['event_type', 'metadata'], f'event_id = {event_id}')
        if result:
            result[event_id]['metadata'] = json.loads(result[event_id]['metadata'])
            return result[event_id]
        else:
            raise FileNotFoundError(f'Event with ID {event_id} not found.')

    def delete(self, event_id: int) -> None:
        self.db_manager.delete(self.table_name, f'event_id = {event_id}')

    def get_all_events(self) -> Dict[int, Dict[str, Any]]:
        results = self.db_manager.select(self.table_name, ['event_id', 'event_type', 'metadata'])
        for event_id, event_data in results.items():
            event_data['metadata'] = json.loads(event_data['metadata'])
        return results

    def describe_table(self) -> Dict[str, str]:
        return self.db_manager.describe(self.table_name)

    def close(self):
        self.db_manager.close()
