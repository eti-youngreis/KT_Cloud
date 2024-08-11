import re


def is_valid_identifier(identifier, length):
    #The function verifies that the length of the identifier string is in the range between 1 and length (inclusive).
    good_length=bool(1 <= len(identifier) <= length)

    #The first character must be an English letter (uppercase or lowercase), followed by letters, numbers, or dashes. The last character must be a letter or number.
    pattern = r'^[a-zA-Z][a-zA-Z0-9-]*[a-zA-Z0-9]$'
    if length == 63:
        pattern = r'^[a-zA-Z][a-zA-Z0-9-.]*[a-zA-Z0-9]$'

    # Checking that the variable matches the regular expression and that it does not contain two consecutive hyphens
    return bool(re.match(pattern, identifier) and '--' not in identifier) and good_length