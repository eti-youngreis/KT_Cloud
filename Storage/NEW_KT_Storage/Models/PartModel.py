import json
from typing import Optional
from datetime import datetime

class PartModel:
    def __init__(self, part_number: int, part_file_path:str,etag: str = None, last_modified: Optional[datetime] = None):
        """יצירת מודל של חלק מסוים עם מספר, גוף תוכן ו-ETag"""
        self.part_number = part_number
        self.etag = etag
        self.last_modified = last_modified
        self.part_file_path=part_file_path

    # def to_sql(self):
    #     # Convert the model instance to a dictionary
    #     data_dict = self.to_dict()
    #     values = '(' + ", ".join(f'\'{json.dumps(v)}\'' if isinstance(v, dict) or isinstance(v, list) else f'\'{v}\'' if isinstance(v, str) else f'\'{str(v)}\''
    #                        for v in data_dict.values()) + ')'
    #     return values


    def to_dict(self) -> dict:
        return {
            'PartNumber': self.part_number,
            'ETag': self.etag,
            'LastModified': self.last_modified.isoformat() if self.last_modified else None,
            'part_file_path':self.part_file_path
        }