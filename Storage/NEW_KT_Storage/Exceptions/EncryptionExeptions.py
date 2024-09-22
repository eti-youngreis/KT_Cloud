
class EncryptionServiceError(Exception):
    """
    Base class for exceptions in the EncryptionService.
    This is the general exception that all specific encryption service-related errors will inherit from.
    """
    pass


class MasterKeyNotFoundError(EncryptionServiceError):
    """
    Exception raised when the master key is not found in the database.
    This exception is specific to the case where the encryption key cannot be retrieved.
    """
    def __init__(self, message="Master key not found in the database."):
        self.message = message
        super().__init__(self.message)


class EncryptionError(EncryptionServiceError):
    """
    Exception raised for errors that occur during encryption or decryption processes.
    This can include any failure to properly encrypt or decrypt data.
    """
    def __init__(self, message="Error occurred during encryption/decryption."):
        self.message = message
        super().__init__(self.message)


class KeyGenerationError(EncryptionServiceError):
    """
    Exception raised when an error occurs during key generation.
    This can be raised if the system fails to create or encrypt a new key.
    """
    def __init__(self, message="Error occurred during key generation."):
        self.message = message
        super().__init__(self.message)


class DatabaseError(EncryptionServiceError):
    """
    Exception raised for database-related errors.
    This can occur when interacting with the database to retrieve or store keys.
    """
    def __init__(self, message="Error occurred while interacting with the database."):
        self.message = message
        super().__init__(self.message)
