class BucketAlreadyExistsError(Exception):
    """Raised when trying to create a bucket that already exists."""
    def __init__(self, bucket_name):
        self.bucket_name = bucket_name
        super().__init__(f"The bucket '{bucket_name}' already exists.")

class InvalidBucketNameError(Exception):
    """Raised when the bucket name is invalid."""
    def __init__(self, bucket_name, message="Invalid bucket name."):
        self.bucket_name = bucket_name
        super().__init__(f"{message} Provided bucket name: '{bucket_name}'.")

class InvalidOwnerError(Exception):
    """Raised when the owner name is invalid."""
    def __init__(self, owner_name, message="Invalid owner name."):
        self.owner_name = owner_name
        super().__init__(f"{message} Provided owner name: '{owner_name}'.")

class BucketNotFoundError(Exception):
    """Raised when trying to access or delete a bucket that does not exist."""
    def __init__(self, bucket_name):
        self.bucket_name = bucket_name
        super().__init__(f"The bucket '{bucket_name}' does not exist.")

class InvalidRegionError(Exception):
    """Raised when the region provided is invalid."""
    def __init__(self, region):
        self.region = region
        super().__init__(f"Invalid region: '{region}'.")
