import uuid
import json
from DB.Scripts.Management import insert_into_management_table, delete_from_management, update_metadata


class Endpoint:
    def __init__(self, cluster_identifier, endpoint_identifier, endpoint_type, static_members, excluded_members):
        """
        Initializes an Endpoint instance.

        Args:
        cluster_identifier (str): The identifier of the DB cluster to which this endpoint belongs
        endpoint_identifier (str): The identifier of the endpoint
        endpoint_type (str): The type of the endpoint (e.g., CUSTOM)
        static_members (list): List of static members associated with the endpoint
        excluded_members (list): List of members excluded from the endpoint
        """
        self.endpoint_identifier = endpoint_identifier
        self.cluster_identifier = cluster_identifier
        self.endpoint_type = endpoint_type
        self.static_members = static_members
        self.excluded_members = excluded_members
        self.counter = 0
        # self.status = 'available'

    def get_metadata(self):
        """
        Retrieves metadata of the endpoint as a JSON string.

        Returns:
        str: JSON string representing the endpoint's metadata
        """
        data = {
            'endpoint_type': self.endpoint_type,
            'static_members': self.static_members,
            'excluded_members': self.excluded_members
        }
        metadata = {k: v for k, v in data.items() if v is not None}
        metadata_json = json.dumps(metadata)
        return metadata_json

    def save_to_db(self, conn=None):
        """
        Saves the endpoint to the database.
        """
        metadata_json = self.get_metadata()
        insert_into_management_table(
            self.__class__.__name__, self.endpoint_identifier, metadata_json, conn)

    def delete(self):
        """
        Deletes the endpoint from the management table.
        """
        delete_from_management(self.__class__.__name__,
                               self.endpoint_identifier)

    def describe(self, status='available'):
        """
        Describes the endpoint with details.

        Args:
        status (str, optional): Status of the endpoint (default: 'available')

        Returns:
        dict: Description of the endpoint
        """
        describe = {
            'DBClusterEndpointIdentifier': self.endpoint_identifier,
            'DBClusterIdentifier': self.cluster_identifier,
            'DBClusterEndpointResourceIdentifier': str(uuid.uuid4()),
            'Endpoint': f'{self.endpoint_identifier}.cluster-{self.cluster_identifier}.example.com',
            'Status': status,
            'EndpointType': 'CUSTOM',
            'CustomEndpointType': self.endpoint_type,
            'StaticMembers': self.static_members,
            'ExcludedMembers': self.excluded_members,
            'DBClusterEndpointArn': f'arn:aws:rds:region:account:dbcluster-endpoint/{self.endpoint_identifier}'
        }
        return describe

    def modify(self, endpoint_type, static_members, excluded_members):
        if endpoint_type != '':
            self.endpoint_type = endpoint_type
            update_metadata(self.__class__.__name__, self.endpoint_identifier,
                            'endpoint_type', self.endpoint_type)
        if static_members is not None:
            self.static_members = static_members
            update_metadata(self.__class__.__name__, self.endpoint_identifier,
                            'static_members', self.static_members)
        if excluded_members is not None:
            self.excluded_members = excluded_members
            update_metadata(self.__class__.__name__, self.endpoint_identifier, 'excluded_members',
                            self.excluded_members)
        return self.describe('modifying')
