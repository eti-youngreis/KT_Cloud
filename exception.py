class AlreadyExistsError(Exception):
    """Raised when an object already exists."""
    pass


class ConnectionError(Exception):
    """Raised when a connection to the database fails."""
    pass


class DatabaseNotFoundError(Exception):
    """Raised when a database is not found."""
    pass


class DBInstanceNotFoundError(Exception):
    """Raised when a database instance is not found."""
    pass


class MissingRequireParamError(Exception):
    """Raised when a required parameter in a function is missing."""
    pass


class ParamValidationError(Exception):
    pass


class InvalidDBInstanceStateError(Exception):
    """Raised when trying to perform an operation on DBInstaace when it is not in the appropriate status for this operation"""
    pass
