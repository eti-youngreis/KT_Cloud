from typing import Dict
import uuid
from DataAccess import ObjectManager


class TagObject:
    def __init__(self, key: str, value: str):
        self.tag_id = str(uuid.uuid1())
        self.key = key
        self.value = value
        self.pk_column = "TagId"
        self.pk_value = self.tag_id

    def to_dict(self) -> Dict:
        """Retrieve the data of the DB cluster as a dictionary."""

        # send relevant attributes in this syntax:
        return ObjectManager.convert_object_attributes_to_dictionary(
            key=self.key,
            value=self.value,
            pk_column=self.pk_column,
            pk_value=self.pk_value,
        )
