from Abc import DBO

class Integration(DBO):
    def create(self, *args, **kwargs):
        """Create a new database object."""
        pass

    def delete(self, *args, **kwargs):
        """Delete an existing database object."""
        pass

    def describe(self, *args, **kwargs):
        """Describe the details of a database object."""
        pass

    def modify(self, *args, **kwargs):
        """Modify an existing database object."""
        pass