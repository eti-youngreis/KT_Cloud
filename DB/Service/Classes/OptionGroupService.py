from DataAccess import DataAccessLayer
from Models import OptionGroupModel
from Abc import DBO
import Validation

class OptionGroupService(DBO):
    def __init__(self, dal: DataAccessLayer):
        self.dal = dal

    def is_valid_engineName(self, engine_name):
        """Check if the engineName is valid."""
        valid_engine_names = {
            "db2-ae", "db2-se", "mariadb", "mysql", "oracle-ee",
            "oracle-ee-cdb", "oracle-se2", "oracle-se2-cdb",
            "postgres", "sqlserver-ee", "sqlserver-se",
            "sqlserver-ex", "sqlserver-web"
        }

        return Validation.string_in_dict(engine_name, valid_engine_names)
    
    def is_valid_optionGroupName(self, option_group_name):
        """
        Check if the optionGroupName is valid based on the pattern and length.

        The optionGroupName must:
        - Be between 1 and 255 characters in length.
        - Start with a letter.
        - Contain only letters, digits, or hyphens.
        - Not contain two consecutive hyphens.
        - End with a letter or digit.

        Args:
            option_group_name (str): The name of the option group to validate.

        Returns:
            bool: True if the optionGroupName is valid, False otherwise.
        """
        pattern = r'^[a-zA-Z](?!.*--)[a-zA-Z0-9-]{0,253}[a-zA-Z0-9]$'
        return Validation.is_valid_length(option_group_name, 1, 255) and Validation.is_valid_pattern(option_group_name, pattern)

    def option_group_exists(self, option_group_name):
        """Check if an option group with the given name already exists."""
        return validations.exist_key_value_in_json_column("object_management", "metadata", "optionGroupName", option_group_name)

    def get_option_group_count(self):###
        """Get the number of option groups in the table."""
        c = conn.cursor()
        c.execute('''
        SELECT COUNT(*) FROM object_management
        WHERE type_object = 'OptionGroup'
        ''')
        return c.fetchone()[0]

    def create(self, engine_name, major_engine_version, option_group_description, option_group_name, tags):
        """Create a new Option Group."""
        if not self.is_valid_engineName(engine_name):
            raise ValueError(f"Invalid engineName: {engine_name}")
        if not self.is_valid_optionGroupName(option_group_name):
            raise ValueError(f"Invalid optionGroupName: {option_group_name}")
        
        
        if option_group_exists(conn, option_group_name):
            return {
                "Error": {
                    "Code": "OptionGroupAlreadyExistsFault",
                    "Message": f"Option group '{option_group_name}' already exists.",
                    "HTTPStatusCode": 400
                }
            }
        if get_option_group_count(conn) >= 20:
            return {
                "Error": {
                    "Code": "OptionGroupQuotaExceededFault",
                    "Message": f"Quota of 20 option groups exceeded for this AWS account.",
                    "HTTPStatusCode": 400
                }
            }
        

        try:
            insert_new_option_group_to_object_management_table(
                conn, engine_name, major_engine_version, option_group_description, option_group_name, tags
            )
            return{
                "CreateOptionGroupResponse": {
                    "ResponseMetadata": {
                        "HTTPStatusCode": 200
                    }
                }
            }
        except OperationalError as e:
            return {
                "Error": {
                    "Code": "InternalError",
                    "Message": f"An internal error occurred: {str(e)}",
                    "HTTPStatusCode": 500
                }
            }
            
    def delete(self, option_group_name):
        """Delete an existing Option Group."""

    def modify(self, option_group_name, apply_immediately, options_to_include, options_to_remove):
        """Modify an existing Option Group."""

    def copy(self, source_option_group_identifier, target_option_group_description, target_option_group_identifier, tags):
        """Copy an Option Group."""

    def describe(self, option_group_name, engine_name, major_engine_version, marker, max_records):
        """Retrieve the details of an Option Group or all Option Groups."""

    def describe_by_engine_name(engine_name, major_engine_version, marker, max_records):
        """Retrieve the details of an Option Group."""

