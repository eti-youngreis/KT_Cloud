import uuid


def validate_part_size(part_size: int):
        """Checks if the part size is positive and non-zero."""
        if part_size <= 0:
            raise ValueError("Part size must be a positive number.")
        
def validate_upload_id(upload_id: str):
        """Checks if the upload_id is a valid UUID."""
        try:
            uuid.UUID(upload_id)
        except ValueError:
            raise ValueError("upload_id must be a valid UUID.")