from Abc import STOE


class PartService(STOE):
    def list(self, part_id, *args, **kwargs):
        """Implement logic to copy the part object."""
        print(f"Copying part with ID: {part_id}")

    def head(self, part_id, *args, **kwargs):
        """Implement logic to restore from the part object."""
        print(f"Restoring part with ID: {part_id}")

    def create(self, *args, **kwargs):
        """Implement logic to create a part."""
        print("Creating a Part")

    def delete(self, *args, **kwargs):
        """Implement logic to delete a part."""
        print("Deleting a Part")

    def get(self, *args, **kwargs):
        """get part."""
        print("Getting a Part")

    def put(self, *args, **kwargs):
        """put part."""
        print("Putting a Part")
