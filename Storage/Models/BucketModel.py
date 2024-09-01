from typing import Set
from Storage.Models.ObjectModel import ObjectModel
class Bucket:
    def __init__(self, name: str) -> None:
        self.name = name
        self.objects: Set[ObjectModel] = set()
        self.encrypt_mode = False

  

