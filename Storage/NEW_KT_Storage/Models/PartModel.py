class PartModel:
    def __init__(self, part_number: int, body: str, etag: str = None, last_modified: Optional[datetime] = None):
        """יצירת מודל של חלק מסוים עם מספר, גוף תוכן ו-ETag"""
        self.part_number = part_number
        self.etag = etag
        self.last_modified = last_modified
        # self.body = body

    def to_dict(self) -> dict:
        return {
            'PartNumber': self.part_number,
            'ETag': self.etag,
            'LastModified': self.last_modified.isoformat() if self.last_modified else None,
            # 'body': self.body
        }