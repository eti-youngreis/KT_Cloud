from typing import Dict, Optional


from typing import Set
from Storage.Models.ObjectModel import ObjectModel


class Bucket:
    def __init__(self, name: str) -> None:
        self.name = name
        self.objects: Set[ObjectModel] = set()
        self.encrypt_mode = False
        
    def is_encrypted(self):
        return self.encrypt_mode
    
    def update_encryption(self, is_encrypted):
        self.encrypt_mode = is_encrypted
