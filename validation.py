from exception import ParamValidationError, MissingRequireParamError


def check_required_params(required_params, kwargs):
    """Check if all required parameters are present in kwargs."""
    for param in required_params:
        if param not in kwargs:
            raise MissingRequireParamError(f"Missing required parameter in input: {param}")


def check_extra_params(all_params, kwargs):
    string_all_params = ", ".join(all_params)
    for param in kwargs:
        if param not in all_params:
            raise ParamValidationError(f"Unknown parameter in input: {param}, must be one of: {string_all_params}")

