from typing import List

class QuotaModel:
    def __init__(self, name: str, resource_type: str, restriction_type: str, limit: int, period: str, 
                usage: int = 0, users: List[str] = None, groups: List[str] = None, roles: List[str] = None):
        """
        :param name: The name of the quota.
        :param resource_type: The resource type (e.g., 'storage', 'bandwidth').
        :param restriction_type: The restriction type (e.g., 'volume', 'amount').
        :param limit: The maximum limit (in some resource units, like megabytes or number of files).
        :param period: The period in which the quota is measured (e.g., 'daily', 'weekly', 'monthly').
        :param usage: Amount of resources already used, initial value is 0.
        :param users: List of user IDs the quota applies to.
        :param groups: List of group IDs the quota applies to.
        :param roles: List of role IDs the quota applies to.
        """
        self.name = name
        self.resource_type = resource_type
        self.restriction_type = restriction_type
        self.limit = limit
        self.period = period
        self.usage = usage
        self.users = users if users is not None else []
        self.groups = groups if groups is not None else []
        self.roles = roles if roles is not None else []

    
    def reset_usage(self):
        self.usage = 0
        
    def check_exceeded(self, usage: int) -> bool:
        return usage>= self.limit

    def __str__(self):
        return (f"Quota(name={self.name}, resource_type={self.resource_type}, restriction_type={self.restriction_type}, "
                f"limit={self.limit}, period={self.period}, usage={self.usage}, users={self.users}, "
                f"groups={self.groups}, roles={self.roles})")

    def to_dict(self) -> dict:
        return {
            'name': self.name,
            'resource_type': self.resource_type,
            'restriction_type': self.restriction_type,
            'limit': self.limit,
            'period': self.period,
            'usage': self.usage,
            'users': self.users,
            'groups': self.groups,
            'roles': self.roles
        }
        
    def add_entity(self, entity_type: str, entity_id: str):
        """
        Add an entity to the quota.

        :param entity_type: The type of entity ('users', 'groups', 'roles').
        :param entity_id: The ID of the entity to be added.
        """
        if entity_type == 'users':
            if entity_id not in self.users:
                self.users.append(entity_id)
        elif entity_type == 'groups':
            if entity_id not in self.groups:
                self.groups.append(entity_id)
        elif entity_type == 'roles':
            if entity_id not in self.roles:
                self.roles.append(entity_id)
        else:
            raise ValueError(f"Invalid entity type: {entity_type}")

    def remove_entity(self, entity_type: str, entity_id: str):
        """
        Remove an entity from the quota.

        :param entity_type: The type of entity ('users', 'groups', 'roles').
        :param entity_id: The ID of the entity to be removed.
        """
        if entity_type == 'users':
            if entity_id in self.users:
                self.users.remove(entity_id)
        elif entity_type == 'groups':
            if entity_id in self.groups:
                self.groups.remove(entity_id)
        elif entity_type == 'roles':
            if entity_id in self.roles:
                self.roles.remove(entity_id)
        else:
            raise ValueError(f"Invalid entity type: {entity_type}")

