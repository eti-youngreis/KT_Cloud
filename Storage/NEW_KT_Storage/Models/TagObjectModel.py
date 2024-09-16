from typing import Dict
from DataAccess import ObjectManager
import uuid


class TagObject:
    def __init__(self, key: str, value: str, pk_value):
        self.id = str(uuid.uuid1())
        self.key = key
        self.value = value
        self.pk_column = "id"
        self.pk_value = self.id

    def to_dict(self) -> Dict:
        """Retrieve the data of the DB cluster as a dictionary."""

        # send relevant attributes in this syntax:
        return ObjectManager.convert_object_attributes_to_dictionary(
            key=self.key,
            value=self.value,
            pk_column=self.pk_column,
            pk_value=self.pk_value,
        )
