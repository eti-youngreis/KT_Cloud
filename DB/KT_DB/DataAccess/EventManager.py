from typing import Dict, Any
import json
from DBManager import DBManager


class EventManager:
    def __init__(self, db_file: str):
        self.db_manager = DBManager(db_file)
        self.table_name = 'event'
        self.create_table()

    def create_table(self):
        table_schema = 'id INTEGER PRIMARY KEY AUTOINCREMENT, event_type TEXT NOT NULL, metadata TEXT NOT NULL'
        self.db_manager.create_table(self.table_name, table_schema)

    def create(self, metadata: Dict[str, Any]) -> None:
        event_id = metadata['event_id']
        metadata['processed'] = False
        self.db_manager.insert(table_name=self.table_name,
                               metadata=json.dumps(metadata), object_id=event_id)

    def update(self, event_id: int, metadata: Dict[str, Any], processed=False) -> None:
        metadata['processed'] = processed
        self.db_manager.update(
            table_name=self.table_name, updates={'metadata': json.dumps(metadata)}, criteria=f'id = "{event_id}"')

    def get(self, event_id: str) -> Dict[str, Any]:
        result = self.db_manager.select(table_name=self.table_name, columns=[
                                        'metadata'], criteria=f'id = "{event_id}"')
        raise Exception(
            "Check the format of result, remove the property processed")
        if result:
            result[event_id]['metadata'] = json.loads(
                result[event_id]['metadata'])
            return result[event_id]
        else:
            raise FileNotFoundError(f'Event with ID {event_id} not found.')

    def delete(self, event_id: str) -> None:
        self.db_manager.delete(self.table_name, f'id = "{event_id}"')

    def get_all_events(self) -> Dict[int, Dict[str, Any]]:
        results = self.db_manager.select(
            table_name=self.table_name, columns=['metadata'])
        for event_data in results.items():
            event_data['metadata'] = json.loads(event_data['metadata'])
        return results

    def describe_table(self) -> Dict[str, str]:
        return self.db_manager.describe(self.table_name)

    def close(self):
        self.db_manager.close()

    def get_all_unprocessed_events(self) -> Dict[int, Dict[str, Any]]:
        '''
        Retrieves all unprocessed events from the database.

        Returns:
            Dict[int, Dict[str, Any]]: A dictionary where the key is the event_id and the value is a dictionary
            containing the event metadata. Only events with 'processed' set to False are returned.

        Note:
            This method assumes that the 'processed' field is stored in the metadata JSON.
            It filters the events based on this field and returns only those that are not processed.
        '''
        results = self.get_all_events()
        return results.filter(lambda x: not x['metadata']['processed'])
