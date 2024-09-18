class DbSnapshotIdentifierNotFoundError(Exception):
    """Raised when a db snapshot identifier is not found."""
    pass


class AlreadyExistsError(Exception):
    """Raised when an object already exists."""
    pass


class DatabaseCreationError(Exception):
    """Raised when there is an error creating the database."""
    pass


class InvalidQueryError(Exception):
    """Raised when a query is not properly constructed or contains syntax errors."""
    pass


class DatabaseCloneError(Exception):
    """Custom exception for database cloning errors."""
    pass


class DatabaseCloneError(Exception):
    """Custom exception for database cloning errors."""
    pass

class DatabaseNotFoundError(Exception):
    """Raised when a database is not found."""
    pass