from typing import List, TypedDict


class Filter(TypedDict):
    
    """
    An object passed as a parameter to certain data requests to help filter the results
    The filter name must be one of the names specified in filter_config.json
    """
    name: str
    values: List[str]