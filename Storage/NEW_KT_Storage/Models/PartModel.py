import json
from typing import Optional
from datetime import datetime

class PartModel:
    def __init__(self, part_number: int, part_file_path:str,etag: str = None, last_modified: Optional[datetime] = None):
        self.part_number = part_number
        self.etag = etag
        self.last_modified = last_modified
        self.part_file_path=part_file_path


    def to_dict(self) -> dict:
        return {
            'PartNumber': self.part_number,
            'ETag': self.etag,
            'LastModified': self.last_modified.isoformat() if self.last_modified else None,
            'part_file_path':self.part_file_path
        }