from typing import Dict, Optional


class Bucket:
    def __init__(self, name: str) -> None:
        self.name = name
        self.objects: Dict = {}  #: Dict[str, Object] = {}
