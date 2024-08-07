from datetime import datetime
import json


def get_json(object_dict):
    def custom_serializer(obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        raise TypeError(f"Type {type(obj)} not serializable")

    object_dict = {k: v for k, v in object_dict.items() if v is not None}
    object_json = json.dumps(object_dict, default=custom_serializer)
    return object_json


def is_valid_json(json_string):
    try:
        json.loads(json_string)
        return True
    except json.JSONDecodeError:
        return False

