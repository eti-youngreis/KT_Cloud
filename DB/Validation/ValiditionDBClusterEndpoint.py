import re


def is_valid_identifier(identifier, length):
    if not 1 <= len(identifier) <= length:
        raise ValueError(f"the length must be between 1 to {length}")

    # ביטוי רגולרי לבדיקת הדרישות הנוספות
    pattern = r'^[a-zA-Z][a-zA-Z0-9-]*[a-zA-Z0-9]$'
    if length == 63:
        pattern = r'^[a-zA-Z][a-zA-Z0-9-.]*[a-zA-Z0-9]$'

    # בדיקה שהמשתנה תואם את הביטוי הרגולרי ושאין בו שתי מקפים רצופים
    if not (re.match(pattern, identifier) and '--' not in identifier):
        raise ValueError("The identity syntax is incorrect")