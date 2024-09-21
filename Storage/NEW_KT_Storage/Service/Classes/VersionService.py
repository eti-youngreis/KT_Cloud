import uuid
from Storage.NEW_KT_Storage.Models.VersionModel import Version
from Storage.NEW_KT_Storage.Service.Abc.STO import STO
from Storage.NEW_KT_Storage.Validation.GeneralValidations import check_required_params
from Storage.NEW_KT_Storage.DataAccess.VersionManager import VersionManager
from typing import Dict
import difflib
import networkx as nx
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')  # Set the backend before importing pyplot


class VersionService(STO):

    def __init__(self, manager: VersionManager):
        self.dal = manager

    def create(self, bucket_name: str, key: str, is_latest: bool, content, last_modified, etag: str, size: int,
               version_id=None, storage_class="STANDARD", owner=None, metadata=None, delete_marker=False,
               checksum=None, encryption_status=None):
        '''Create a new Version.'''

        required_params = ["bucket_name", "key"]
        if not check_required_params(required_params, locals()):
            raise ValueError("Missing required parameters")

        if size and size < 0:
            raise ValueError("Size must be a positive integer")

        if bucket_name is None or key is None:
            raise ValueError("bucket_name and key must not be None")

        # Generate a unique version ID using UUID
        # version_id = f"v{str(uuid.uuid4())}"
        if version_id is None:
            version_id = f"v{str(uuid.uuid4())}"
        else:
            # Check if version already exists
            existing_version = self.dal.get_version(bucket_name=bucket_name, key=key, version_id=version_id)
            if existing_version:
                raise ValueError(f"Version with ID '{version_id}' already exists.")

        # Create the Version model
        version = Version(
            pk_value = str(uuid.uuid4()),
            bucket_name=bucket_name,
            object_key=key,
            version_id=version_id,
            content=content,
            is_latest=is_latest,
            last_modified=last_modified,
            etag=etag,
            size=size,
            storage_class=storage_class,
            owner=owner
        )
        # Save physical object using StorageManager
        self.dal.create_in_memory_version(version, bucket_name, key)

        return {"status": "success", "message": "Version object created successfully", "version_id": version_id}

    def get(self, bucket_name: str, key: str, version_id: str) -> Version:
        '''Get a Version.'''

        required_params = ["bucket_name", "key", "version_id"]
        if not check_required_params(required_params, locals()):
            raise ValueError("Missing required parameters for retrieving version.")

        try:
            # Use ObjectManager to retrieve the version object
            version = self.dal.get_version(bucket_name=bucket_name, key=key, version_id=version_id)
            return version
        except FileNotFoundError:
            raise ValueError(f"Version object not found for bucket '{bucket_name}', key '{key}', version '{version_id}'.")

        return version

    def delete(self, bucket_name: str, key: str, version_id: str):
        '''Delete an existing Version.'''

        # Validate parameters
        required_params = ["bucket_name", "key", "version_id"]
        if not check_required_params(required_params, locals()):
            raise ValueError("Missing required parameters for deleting version.")

        # Attempt to delete the version
        try:
            # Call the delete_version method from the VersionManager
            self.dal.delete_version(bucket_name=bucket_name, key=key, version_id=version_id)
        except FileNotFoundError as e:
            raise ValueError(str(e))  # Raise a ValueError if the version does not exist

        return {"status": "success", "message": "Version object deleted successfully"}

    def describe(self, bucket_name: str, key: str, version_id: str) -> Dict:
        '''Describe the details of a Version.'''

        # Validate parameters
        required_params = ["bucket_name", "key", "version_id"]
        if not check_required_params(required_params, locals()):
            raise ValueError("Missing required parameters for describing version.")

        # Use ObjectManager to retrieve the version object details
        version = self.dal.describeVersionObject(bucket_name=bucket_name, object_key=key, version_id=version_id)

        if not version:
            raise ValueError("Version not found.")

        return version.to_dict()

    # def put(self, bucket_name: str, key: str, version_id: str, updates: Dict):
    #     '''Modify an existing Version.'''
    #
    #     # Validate parameters
    #     required_params = ["bucket_name", "key", "version_id", "updates"]
    #     if not GeneralValidation.check_required_params(required_params, locals()):
    #         raise ValueError("Missing required parameters for updating version.")
    #
    #     if not updates:
    #         raise ValueError("Updates must be a non-empty dictionary.")
    #
    #     # Modify physical object (e.g., update file attributes or contents in storage)
    #     # Placeholder: call StorageManager.update_file()
    #
    #     # Update in memory using ObjectManager
    #     self.dal.putVersionObject(bucket_name=bucket_name, object_key=key, version_id=version_id, updates=updates)
    #
    #     return {"status": "success", "message": "Version object updated successfully"}



    def put(self, bucket_name: str, key: str, version_id: str, updates: Dict):
        '''Modify an existing Version.'''
        # Validate parameters
        required_params = ["bucket_name", "key", "version_id", "updates"]
        if not check_required_params(required_params, locals()):
            raise ValueError("Missing required parameters for updating version.")

        if not updates:
            raise ValueError("Updates must be a non-empty dictionary.")

        # Modify physical object (if needed)
        # Placeholder: call StorageManager.update_file()

        # Update in memory using ObjectManager
        self.dal.put_version_object(bucket_name=bucket_name, object_key=key, version_id=version_id, updates=updates)

        return {"status": "success", "message": "Version object updated successfully"}

    def analyze_version_changes(self, bucket_name: str, object_key: str, version_id1: str, version_id2: str):
        
        try:
            version1_data = self.get(bucket_name, object_key, version_id1)
            version2_data = self.get(bucket_name, object_key, version_id2)
        except Exception as e:
            print("Version didn't found in database:", e)

        if not version1_data or not version2_data:
            raise ValueError("One or both versions not found")

        # Extract the content from the returned data (content is in index 2)
        version1_content = version1_data[3] if isinstance(version1_data, list) else version1_data[0]["content"]
        version2_content = version2_data[3] if isinstance(version2_data, list) else version2_data[0]["content"]

        # Ensure the content is a string and not empty
        if not isinstance(version1_content, str):
            raise ValueError("Version 1 content is not a valid string.")
        if not isinstance(version2_content, str):
            raise ValueError("Version 2 content is not a valid string.")

        # Split the content into lines for comparison
        content1 = version1_content.splitlines()
        content2 = version2_content.splitlines()

        # Compute differences
        differ = difflib.Differ()
        diff = list(differ.compare(content1, content2))

        # Display changes
        print(f"Changes between version {version_id1} and {version_id2}:")
        for line in diff:
            if line.startswith('+ '):
                print(f"\033[92m{line}\033[0m")  # Green for additions
            elif line.startswith('- '):
                print(f"\033[91m{line}\033[0m")  # Red for deletions
            elif line.startswith('? '):
                continue  # Skip the hints
            else:
                print(line)
    def visualize_version_history(self, bucket_name: str, object_key: str):
        """
        Visualize the version history of an object using a directed graph.
        :param bucket_name: The name of the bucket.
        :param object_key: The key of the object.
        """
        # Fetch all versions of the object from memory
        all_versions = self.dal.object_manager.get_all_objects_from_memory(Version.object_name)

        # Filter versions belonging to the specific object
        versions = [version for version in all_versions if version[1] == bucket_name and version[4] == object_key]

        if not versions:
            raise ValueError(f"No versions found for object '{object_key}' in bucket '{bucket_name}'.")

        # Create a graph with NetworkX
        G = nx.DiGraph()

        for i, version in enumerate(versions):
            # Add a node for each version (add more details if needed)
            label = f"V{i + 1}\n{version[3]}"  # assuming last_modified is at index 3
            G.add_node(version[2], label=label)  # version_id is at index 2

            # Create edge from previous version to the current version
            if i > 0:
                G.add_edge(versions[i - 1][2], version[2])

        # Use a circular layout to ensure all nodes are spread out
        pos = nx.circular_layout(G)

        # Draw the graph
        plt.figure(figsize=(12, 8))
        nx.draw(G, pos, with_labels=True, node_color='lightblue', node_size=5000, font_size=10, font_weight='bold',
                arrows=True)
        labels = nx.get_node_attributes(G, 'label')
        nx.draw_networkx_labels(G, pos, labels, font_size=8)
        plt.title(f"Version History for {object_key} in {bucket_name}")
        plt.axis('off')
        plt.tight_layout()

        # Save the figure to a file
        plt.savefig('version_history.png')
        plt.close()
