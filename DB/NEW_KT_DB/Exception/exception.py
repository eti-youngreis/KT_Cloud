class AlreadyExistsError(Exception):
    """Raised when an object already exists."""
    pass


class DatabaseCreationError(Exception):
    """Raised when there is an error creating the database."""
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


class InvalidDBInstanceStateError(Exception):
    """Raised when trying to perform an operation on DBInstaace when it is not in the appropriate status for this operation"""
    pass


class ParamValidationError(Exception):
    pass


class StartNonStoppedDBInstance(Exception):
    """Raised when trying to start non stopped db instance"""
    pass


class DatabaseCloneError(Exception):
    """Custom exception for database cloning errors."""
    pass

class DbSnapshotIdentifierNotFoundError(Exception):
    """Raised when a db snapshot identifier is not found."""
    pass

class InvalidQueryError(Exception):
    """Raised when a query is not properly constructed or contains syntax errors."""
    pass

class InvalidQueryError(Exception):
    """Raised when a query is not properly constructed or contains syntax errors."""
    pass
