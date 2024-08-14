class Filter:
    
    """
    An object passed as a parameter to certain data requests to help filter the results
    The filter name must be one of the names specified in filter_config.json
    """
    def __init__(self, name, values):
        self.name = name
        self.values = values