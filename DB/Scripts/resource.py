from exception import *
from abc import ABC, abstractmethod


# Important note: This class will serve as a base class for all types of resources in the future


class Resource(ABC):
    """
    Abstract base class for managing resources with tags.

    Attributes:
        resource_name (str): The name of the resource.
        tag_list (dict): A dictionary of tags associated with the resource.

    Methods:
        __init__(): Initializes the class with an empty resource name and an empty tag list.
        add_tags(list_tags): Adds tags to the resource's tag list.
        remove_tags(tag_keys): Removes tags from the resource's tag list based on keys.
        valid_tag(tag): Checks if the tag contains only valid keys.
    """

    def __init__(self):
        """
        Initializes the class with an empty resource name and an empty tag list.
        """
        self.resource_name = None
        self.tag_list = {}

    def add_tags(self, list_tags):
        """
        Adds tags to the resource's tag list.

        Parameters:
            list_tags (list): A list of tags (dictionaries) to add.

        Returns:
            dict: The updated dictionary of the resource's tags.

        Raises:
            ParamValidationError: If the tag does not contain only 'Key' and 'Value' keys.
            InvalidParameterValue: If the tag key is not between 1 and 128 characters in length.
        """
        for tag in list_tags:
            if not self.__valid_tag(tag):
                raise ParamValidationError("Parameter must be one of: Key, Value")
            if not tag.get('Key') or len(tag.get('Key')) > 128:
                raise InvalidParameterValue("Tag keys must be between 1 and 128 characters in length")
            self.tag_list[tag.get('Key')] = tag.get('Value')
        return self.tag_list

    def remove_tags(self, tag_keys):
        """
        Removes tags from the resource's tag list based on keys.

        Parameters:
            tag_keys (list): A list of tag keys to remove.
        """
        for key in tag_keys:
            self.tag_list.pop(key, None)

    def __valid_tag(self, tag):
        """
        Checks if the tag contains only valid keys ('Key', 'Value').

        Parameters:
            tag (dict): The tag to check.

        Returns:
            bool: True if the tag is valid, False otherwise.
        """
        return set(tag.keys()).issubset(['Key', 'Value'])