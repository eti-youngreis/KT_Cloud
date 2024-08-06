from exception import *

# Contains all the different instances running in the background
resource_object = {"db": {}, "cluster": {}, "snapshot": {}}


def __get_resource_by_arn(arn: str):
    """ The function will return the resource with this ARN. If it does not exist, it will raise an appropriate error."""
    # Split the ARN into its components
    list_arn = arn.split(":")
    # Validate the ARN format
    if len(list_arn) != 7 or list_arn[0] != "arn" or list_arn[1] != "vast" or list_arn[2] != "rds" or list_arn[3] != "eu-west-1":
        raise InvalidParameterValue(f"Invalid resource name: {arn}")

    resource_type = list_arn[5]
    # Check if the resource type exists in the resource dictionary
    if resource_type not in resource_object.keys():
        raise InvalidParameterValue(f"Invalid resource name: {arn}")
    resource_name = list_arn[6]
    # Get the resource from the ARN
    resource = resource_object[resource_type].get(resource_name)
    if not resource:
        raise InvalidParameterValue(f"Unable to find a {resource_type} matching the resource name: {arn}")
    return resource


def add_tags_to_resource(**kwargs):
    required_params = ["Tags", "ResourceName"]
    all_params = []
    all_params.extend(required_params)
    __valid_params(all_params, kwargs)
    __check_required_params(required_params, kwargs)
    resource = __get_resource_by_arn(kwargs["ResourceName"])
    list_tag = resource.add_tags(kwargs["Tags"])
    return __convert_to_list(list_tag)


def list_tags_for_resource(**kwargs):
    required_params = ["ResourceName"]
    all_params = ["Filters"]
    all_params.extend(required_params)
    __valid_params(all_params, kwargs)
    __check_required_params(required_params, kwargs)
    resource = __get_resource_by_arn(kwargs["ResourceName"])
    list_tag = resource.tag_list
    filters = kwargs.get("Filters",None)
    if filters:
        __check_filters_validation(filters)
        for filter in filters:
            if filter['Name'] == 'Key':
                for val in filter['Values']:
                    list_tag.pop(val,None)
            elif filter['Name' == 'Value']:
                for val in filter['Values']:
                    key_to_remove = next((key for key, value in list_tag.items() if value == val), None)
                    if key_to_remove is not None:
                        del list_tag[key_to_remove]
        
    return __convert_to_list(list_tag)


def remove_tags_from_resource(**kwargs):
    required_params = ["ResourceName", "TagKeys"]
    all_params = []
    all_params.extend(required_params)
    __valid_params(all_params, kwargs)
    __check_required_params(required_params, kwargs)
    resource = __get_resource_by_arn(kwargs["ResourceName"])
    resource.remove_tags(kwargs["TagKeys"])


def __check_required_params(required_params, kwargs):
    """
    Check if all required parameters are present in kwargs.
    """
    for param in required_params:
        if param not in kwargs:
            raise ParamValidationError(f"Missing required parameter in input: {param}")


def __valid_params(all_params, kwargs):
    """
    Check if all parameters in kwargs are valid.
    """
    string_all_params = ", ".join(all_params)
    for param in kwargs:
        if param not in all_params:
            raise ParamValidationError(f"Unknown parameter in input: {param}, must be one of: {string_all_params}")

def __check_filters_validation(filters):
    if not isinstance(filters, list):
        raise ParamValidationError("Filters must be a list")
    
    for filter_item in filters:
        if not isinstance(filter_item, dict):
            raise ParamValidationError("Each filter must be a dictionary")
        if 'Name' not in filter_item:
            raise ParamValidationError("Each filter dictionary must contain the key 'Name'")
        if 'Values' not in filter_item:
            raise ParamValidationError("Each filter dictionary must contain the key 'Values'")
        if not isinstance(filter_item['Name'], str):
            raise ParamValidationError("The value of 'Name' must be a string")
        if not isinstance(filter_item['Values'], list):
            raise ParamValidationError("The value of 'Values' must be a list")
        for value in filter_item['Values']:
            if not isinstance(value, str):
                raise ParamValidationError("Each value in 'Values' must be a string")

def __convert_to_list(tag_list: dict):
    """
    Convert a tag dictionary to a list of tag dictionaries.
    """
    list_of_tags = []
    for key in tag_list.keys():
        tag = {"Key": key, "Value": tag_list[key]}
        list_of_tags.append(tag)
    return list_of_tags