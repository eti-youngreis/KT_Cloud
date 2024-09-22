from datetime import datetime
import json
from DB.NEW_KT_DB.DataAccess.ObjectManager import ObjectManager


class DBProxy:
    """
    The DBProxy class represents a database proxy object and manages the related information.
    It contains fields corresponding to the database table structure and allows conversion
    of the object to a dictionary or an SQL string for storage purposes.
    """

    # Table structure in the database
    table_structure = """
    db_proxy_name VARCHAR(100) PRIMARY KEY NOT NULL,
    engine_family TEXT NOT NULL,
    role_arn TEXT NOT NULL,
    auth TEXT NOT NULL,
    vpc_subnet_ids TEXT NOT NULL,
    vpc_security_group_ids TEXT NULL,
    require_TLS TEXT NULL,
    idle_client_timeout INT NULL,
    debug_logging TEXT NULL,
    tags TEXT NULL,
    endpoint TEXT NOT NULL,
    status TEXT NOT NULL,
    create_date TEXT NOT NULL,
    update_date TEXT NOT NULL
    """

    # Object name and primary key column in the database table
    object_name = 'DBProxy'
    pk_column = 'db_proxy_name'

    def __init__(self, **kwargs) -> None:
        """
        Initializes a DBProxy object with the values passed as arguments.
        Default values are used if some fields are not provided.
        """
        self.db_proxy_name = kwargs.get('db_proxy_name', None)
        self.engine_family = kwargs.get('engine_family', None)
        self.role_arn = kwargs.get('role_arn', None)
        self.auth = kwargs.get('auth', None)  # Contains authentication details
        self.vpc_subnet_ids = kwargs.get('vpc_subnet_ids', None)  # List of VPC subnet IDs
        self.vpc_security_group_ids = kwargs.get('vpc_security_group_ids', None)  # List of security group IDs
        self.require_TLS = kwargs.get('require_TLS', None)  # Whether TLS is required
        self.idle_client_timeout = kwargs.get('idle_client_timeout', None)  # Client idle timeout before disconnect
        self.debug_logging = kwargs.get('debug_logging', None)  # Enables debug logging
        self.tags = kwargs.get('tags', None)  # Additional tags related to the proxy
        self.endpoint = 'db.proxy' + self.db_proxy_name + '.rds.amazonaws.com'  # Proxy endpoint URL
        self.status = 'available'  # Default status of the proxy
        self.create_date = kwargs.get('create_date', datetime.now())  # Proxy creation date
        self.update_date = kwargs.get('update_date', datetime.now())  # Last update date of the proxy

    def to_dict(self):
        """
        Converts the DBProxy object to a dictionary where each field becomes a key-value pair.
        This is useful for preparing the data before storing it in the database or other systems.

        Returns:
            dict: A dictionary representing the fields and their values.
        """
        return ObjectManager.convert_object_attributes_to_dictionary(
            db_proxy_name=self.db_proxy_name,
            engine_family=self.engine_family,
            role_arn=self.role_arn,
            auth=self.auth,
            vpc_subnet_ids=self.vpc_subnet_ids,
            vpc_security_group_ids=self.vpc_security_group_ids,
            require_TLS=self.require_TLS,
            idle_client_timeout=self.idle_client_timeout,
            debug_logging=self.debug_logging,
            tags=self.tags,
            endpoint=self.endpoint,
            status=self.status,
            create_date=self.create_date.isoformat(),  # Convert date to ISO format string
            update_date=self.update_date.isoformat(),  # Convert update date to ISO format string
        )

    def to_sql(self):
        """
        Converts the DBProxy object into an SQL string suitable for inserting the data into a database.
        It converts the dictionary of object attributes into an SQL-compatible string.

        Returns:
            str: SQL string representing the proxy values ready for insertion into the table.
        """
        data = self.to_dict()  # Convert the object to a dictionary
        values_to_sql = []  # List to store SQL-formatted values

        # Iterate over each key-value pair in the dictionary
        for key, value in data.items():
            # Convert lists and dictionaries to JSON strings
            if isinstance(value, dict) or isinstance(value, list):
                value = json.dumps(value)
            # Add each value to the SQL string, using f-string formatting
            values_to_sql.append(f"'{value}'")

        # Return the SQL-formatted string with all values
        return '(' + ', '.join(values_to_sql) + ')'
