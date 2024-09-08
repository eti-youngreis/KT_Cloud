from typing import List, Dict
from DB_UserAdministration.Models.DBUserGroupModel import UserGroupModel
from DB.Validation.Validation import is_valid_user_group_name, is_valid_number
from DB_UserAdministration.DataAccess.UserGroupManager import UserGroupManager
from DB_UserAdministration.Services.PolicyService import PolicyService
from DB_UserAdministration.DataAccess.PolicyManager import PolicyManager
from DB_UserAdministration.Services.UserService import userService,UserManager

class UserGroupService:
    """
    Service class to manage user groups, including creation, deletion, update, and membership management.
    """

    def __init__(self, dal: UserGroupManager):
        """
        Initialize the UserGroupService with a DataAccessLayer (DAL) instance.
        
        Args:
            dal (DBManager): The data access layer for interacting with the database.
        """
        self.dal = dal

    def create(self, group_name: str):
        """
        Create a new user group.

        Args:
            group_name (str): The name of the group to create.

        Returns:
            dict: A dictionary representing the newly created group if the operation is successful.

        Raises:
            ValueError: If the group name is not valid or if the group already exists.
        """
        if not is_valid_user_group_name(group_name):
            raise ValueError(f"group_name {group_name} is not valid")
        if self.dal.is_identifier_exist(group_name):
            raise ValueError(f"UserGroup with NAME '{group_name}' already exists.")
        group = UserGroupModel(group_name)
        self.dal.create( group.to_dict(),group_name)
        return {'Group':group.to_dict()}

    def delete(self, group_name: str):
        """
        Delete an existing user group.
        
        Args:
            group_name (str): The name of the group to delete.

        Raises:
            ValueError: If the group contains users or has attached policies.
        """
        data = self.get(group_name)
        print('data[users]:',data['users'])
        if data['users'] != [] or data['policies'] != []:
            raise ValueError("The group must not contain any users or have any attached policies.")
        self.dal.delete(group_name)

    def update(self, group_name: str, new_group_name: str = None):
        """
        Update the name of an existing user group.
        
        Args:
            group_name (str): The current name of the group.
            new_group_name (str, optional): The new name to assign to the group. If not provided, the group name is not changed.
        
        Raises:
            ValueError: If the new group name is not valid.
        """
        if new_group_name is None:
            return
        if not is_valid_user_group_name(new_group_name):
            raise ValueError(f"group_name {new_group_name} is not valid")
        group = self.get(group_name)
        print('befor update group:',group)
        group['name'] = new_group_name
        print('after update group:',group)
        self.dal.update(group_name, group)

    def get(self, group_name: str) -> Dict:
        """
        Retrieve the details of a user group.
        
        Args:
            group_name (str): The name of the group to retrieve.
        
        Returns:
            Dict: A dictionary containing the group details.
        
        Raises:
            ValueError: If the group does not exist.
        """
        return self.dal.get(group_name)

    def get_group_with_users(self, group_name: str, max_items: int = 100) -> Dict:
        """
        Retrieve a user group along with a limited list of users.
        
        Args:
            group_name (str): The name of the group to retrieve.
            max_items (int, optional): The maximum number of users to retrieve. Defaults to 100.
        
        Returns:
            Dict: A dictionary containing the group and its users.
        """
        response = {}
        data = self.get(group_name)
        user_obj=userService(UserManager(self.get_db_name(self.dal.db_manager.connection)))
        users_obj = [user_obj.get_user(user) for user in data["users"][:max_items]]
        user_obj.dal.close()
        response['Group'] = data
        response['Users'] = users_obj
        return response

    def list(self, max_items: int = 100) -> List[Dict]:
        """
        List all user groups with a limit on the number of groups returned.
        
        Args:
            max_items (int, optional): The maximum number of groups to list. Defaults to 100.
        
        Returns:
            List[Dict]: A list of dictionaries, each representing a user group.
        
        Raises:
            ValueError: If max_items is not between 1 and 1000.
        """
        if not is_valid_number(max_items,1,100):
            raise ValueError("max_items must be between 1 to 1000.")
        data = self.dal.get_all_groups()
        result = list(data.values())
        return result[:max_items]

    def get_db_name(self,connection):
        cursor = connection.cursor()
        cursor.execute("PRAGMA database_list;")
        result = cursor.fetchall()
        for db in result:
            if db[1] == 'main':  # 'main' is the primary database
                return db[2]  # db[2] contains the file name
        return None

    def add_member(self, group_name: str, user_id: str):
        """
        Add a user to a user group.
        
        Args:
            group_name (str): The name of the group.
            user_id (str): The ID of the user to add.
        
        Raises:
            ValueError: If the user is already in the group or does not exist.
        """
        group = self.get(group_name)
        user_service=userService(UserManager(self.get_db_name(self.dal.db_manager.connection)))
        user_service.get_user(user_id)
        user_service.dal.close()
        if user_id in group['users']:
            raise ValueError(f"User '{user_id}' is already in group '{group_name}'.")
        group['users']=str(list(group['users']).append(user_id))
        self.dal.update(group_name,group)

    def remove_member(self, group_name: str, user_id: str):
        """
        Remove a user from a user group.
        
        Args:
            group_name (str): The name of the group.
            user_id (str): The ID of the user to remove.
        
        Raises:
            ValueError: If the user is not in the group.
        """
        group = self.get(group_name)
        if user_id not in group['users']:
            raise ValueError(f"User '{user_id}' is not in group '{group_name}'.")
        group['users']=str(list(group['users']).remove(user_id))
        self.dal.update(group_name, group)

    def assign_policy(self, group_name: str, policy: str):
        """
        Assign a policy to a user group.
        
        Args:
            group_name (str): The name of the group.
            policy (str): The name of the policy to assign.
        
        Raises:
            ValueError: If the policy is already assigned to the group or does not exist.
        """
        group = self.get(group_name)
        policy_service=PolicyService(PolicyManager(self.get_db_name(self.dal.db_manager.connection)))
        policy_service.get(policy)
        policy_service.policy_manager.close()
        if policy in group['policies']:
            raise ValueError(f"Policy '{policy}' is already assigned to group '{group_name}'.")
        group['policies']=str(list(group['policies']).append(policy))
        self.dal.update(group_name,group)

    def revoke_policy(self, group_name: str, policy: str):
        """
        Revoke a policy from a user group.
        
        Args:
            group_name (str): The name of the group.
            policy (str): The name of the policy to revoke.
        
        Raises:
            ValueError: If the policy is not assigned to the group.
        """
        group = self.get(group_name)
        if policy not in group['policies']:
            raise ValueError(f"Policy '{policy}' is not assigned to group '{group_name}'.")
        group['policies']=str(list(group['policies']).remove(policy))
        self.dal.update(group_name, group)
