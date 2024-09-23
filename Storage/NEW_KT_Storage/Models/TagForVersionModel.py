from typing import Dict
import uuid
from DataAccess.ObjectManager import ObjectManager
import json
from datetime import datetime


class TagForVersion:
    OBJECT_NAME = "TagForVersion"
    TABLE_STRUCTURE = ", ".join(
        ["TagKey TEXT", "VersionId TEXT","PRIMARY KEY (TagKey, VersionId)"]
    )

    def __init__(self, tag_key: str, version_id: str) -> None:
        self.tag_key = tag_key
        self.version_id = version_id

    def to_dict(self) -> Dict:
        """Retrieve the data of the DB cluster as a dictionary."""
        return ObjectManager.convert_object_attributes_to_dictionary(
            tag_key=self.tag_key,
            version_id=self.version_id,
        )

    def to_sql(self) -> str:
        """Convert the TagForVersion instance to a SQL-friendly format."""
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
        """Convert the TagForVersion instance to a JSON string."""
        try:
            return json.dumps(self.to_dict(), indent=4)
        except Exception as e:
            print(f"Error converting to JSON string: {e}")
            return None
