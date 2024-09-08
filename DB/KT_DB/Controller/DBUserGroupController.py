from typing import List, Dict
from Service.Classes.DBUserGroupService import UserGroupService

class UserGroupController:
    def __init__(self, service: UserGroupService):
        """
        Initialize the UserGroupController with a UserGroupService instance.

        Args:
            service (UserGroupService): An instance of UserGroupService to handle group operations.
        """
        self.service = service

    def create_group(self, group_name: str):
        """
        Create a new user group.

        Args:
            group_name (str): The name of the group to create.

        Returns:
            dict: The newly created group.
        
        Raises:
            ValueError: If the group name is invalid or the group already exists.
        """
        return self.service.create(group_name)

    def delete_group(self, group_name: str):
        """
        Delete an existing user group.

        Args:
            group_name (str): The ID name of the group to delete.

        Raises:
            ValueError: If the group does not exist.
        """
        self.service.delete(group_name)

    def update_group(self, group_name: str, new_group_name: str = None):
        """
        Update the name of an existing user group.

        Args:
            group_name (str): The current name of the group.
            new_group_name (str, optional): The new name for the group. Defaults to None.
        
        Raises:
            ValueError: If the group does not exist or the new group name is invalid.
        """
        self.service.update(group_name, new_group_name)

    def get_group(self, group_name: str, max_items: int = 100) -> Dict:
        """
        Retrieve a user group along with its members.

        Args:
            group_name (str): The name of the group to retrieve.
            max_items (int, optional): The maximum number of members to retrieve. Defaults to 100.

        Returns:
            dict: A dictionary containing the group's details and its members.
        
        Raises:
            ValueError: If the group does not exist.
        """
        return self.service.get_group_with_users(group_name, max_items)

    def list_groups(self, max_items: int = 100) -> List[Dict]:
        """
        List all user groups.

        Args:
            max_items (int, optional): The maximum number of groups to retrieve. Defaults to 100.

        Returns:
            List[Dict]: A list of dictionaries, each representing a group.
        """
        return self.service.list(max_items)

    def add_member_to_group(self, group_name: str, user_id: str):
        """
        Add a member to an existing user group.

        Args:
            group_name (str): The name of the group.
            user_id (str): The ID of the user to add.

        Raises:
            ValueError: If the group or user does not exist.
        """
        self.service.add_member(group_name, user_id)

    def remove_member_from_group(self, group_name: str, user_id: str):
        """
        Remove a member from an existing user group.

        Args:
            group_name (str): The name of the group.
            user_id (str): The ID of the user to remove.

        Raises:
            ValueError: If the group or user does not exist.
        """
        self.service.remove_member(group_name, user_id)

    def assign_permission_to_group(self, group_name: str, policy: str):
        """
        Assign a permission policy to a user group.

        Args:
            group_name (str): The name of the group.
            policy (str): The policy to assign to the group.

        Raises:
            ValueError: If the group or policy does not exist.
        """
        self.service.assign_policy(group_name, policy)

    def revoke_permission_from_group(self, group_name: str, policy: str):
        """
        Revoke a permission policy from a user group.

        Args:
            group_name (str): The name of the group.
            policy (str): The policy to revoke from the group.

        Raises:
            ValueError: If the group or policy does not exist.
        """
        self.service.revoke_policy(group_name, policy)
