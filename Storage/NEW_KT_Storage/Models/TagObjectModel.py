from typing import Dict
import uuid
from DataAccess.ObjectManager import ObjectManager
import json
from datetime import datetime


class TagObject:
    PK_COULMN = "Key"
    OBJECT_NAME = "Tag"
    def __init__(self, key: str, value: str):
        self.key = key
        self.value = value

    def to_dict(self) -> Dict:
        """Retrieve the data of the DB cluster as a dictionary."""

        return ObjectManager.convert_object_attributes_to_dictionary(
            key=self.key,
            value=self.value,
        )

    def to_sql(self) -> str:
        """Convert the TagObject instance to a SQL-friendly format."""
        data_dict = self.to_dict()
        try:
            values = (
                "("
                + ", ".join(
                    (
                        f"'{json.dumps(v)}'"
                        if isinstance(v, dict) or isinstance(v, list)
                        else f"'{v}'" if isinstance(v, str) else f"'{str(v)}'"
                    )
                    for v in data_dict.values()
                )
                + ")"
            )
            return values
        except Exception as e:
            print(f"Error converting to SQL format: {e}")
            return None

    def __str__(self) -> str:
        """Convert the TagObject instance to a JSON string."""
        try:
            return json.dumps(self.to_dict(), indent=4)
        except Exception as e:
            print(f"Error converting to JSON string: {e}")
            return None
