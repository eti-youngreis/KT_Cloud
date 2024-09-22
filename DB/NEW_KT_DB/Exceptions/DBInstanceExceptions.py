class DbSnapshotIdentifierNotFoundError(Exception):
    """Raised when a db snapshot identifier is not found."""
    def __init__(self, snapshot_id):
        self.message = f"Database snapshot with identifier '{snapshot_id}' not found."
        super().__init__(self.message)

class AlreadyExistsError(Exception):
    """Raised when an object already exists."""
    def __init__(self, object_type, identifier):
        self.message = f"{object_type} with identifier '{identifier}' already exists."
        super().__init__(self.message)

class DatabaseCreationError(Exception):
    """Raised when there is an error creating the database."""
    def __init__(self, db_name, reason):
        self.message = f"Failed to create database '{db_name}'. Reason: {reason}"
        super().__init__(self.message)

class InvalidQueryError(Exception):
    """Raised when a query is not properly constructed or contains syntax errors."""
    def __init__(self, query, error_details):
        self.message = f"Invalid query: '{query}'. Error details: {error_details}"
        super().__init__(self.message)

class DatabaseCloneError(Exception):
    """Custom exception for database cloning errors."""
    def __init__(self, source_db, target_db, reason):
        self.message = f"Failed to clone database from '{source_db}' to '{target_db}'. Reason: {reason}"
        super().__init__(self.message)

class DatabaseNotFoundError(Exception):
    """Raised when a database is not found."""
    def __init__(self, db_name):
        self.message = f"Database '{db_name}' not found."
        super().__init__(self.message)
