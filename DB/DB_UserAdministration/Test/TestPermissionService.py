# import pytest
# from unittest.mock import MagicMock
# import sys
# import os 
# sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
# from Models.permissionModel import Action, Resource, Effect, Permission
# from DataAccess.permissionManager import PermissionManager


# class TestPermissionService:

#     @pytest.fixture
#     def permission_manager(self):
#         # Mock DAL object
#         dal = MagicMock()
#         pm = PermissionManager(dal)
#         return pm
    
#     def test_create_permission_success(self, permission_manager):
#         # Arrange
#         action = Action.READ
#         resource = Resource.BUCKET
#         effect = Effect.ALLOW
        
#         # Mock the is_exit_permission method to return False (permission doesn't exist)
#         permission_manager.is_exit_permission = MagicMock(return_value=False)
#         # Mock the dal's create_permission method to simulate a successful permission creation
#         permission_manager.dal.create_permission = MagicMock(return_value={
#             'action': action.value, 
#             'resource': resource.value, 
#             'effect': effect.value
#         })

#         # Act
#         result = permission_manager.create_permission(action, resource, effect)

#         # Assert
#         assert result == {'action': action.value, 'resource': resource.value, 'effect': effect.value}
#         permission_manager.is_exit_permission.assert_called_once_with(action, resource, effect)
#         permission_manager.dal.create_permission.assert_called_once_with({
#             'action': action.value,
#             'resource': resource.value,
#             'effect': effect.value
#         })

#     def test_create_permission_already_exists(self, permission_manager):
#         # Arrange
#         action = Action.WRITE
#         resource = Resource.DATABASE
#         effect = Effect.DENY
        
#         # Mock the is_exit_permission method to return True (permission already exists)
#         permission_manager.is_exit_permission = MagicMock(return_value=True)

#         # Act and Assert
#         with pytest.raises(ValueError, match=f"Permission '{action}, {resource}, {effect}' already exists."):
#             permission_manager.create_permission(action, resource, effect)
        
#         permission_manager.is_exit_permission.assert_called_once_with(action, resource, effect)
#         permission_manager.dal.create_permission.assert_not_called()
# import pytest
# from unittest.mock import MagicMock
# import sqlite3
# import tempfile
# import os
# import sys
# from DataAccess.permissionManager import PermissionManager
import pytest
from unittest.mock import MagicMock
import sys
import os 
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from Models.permissionModel import Action, Resource, Effect
# from DataAccess.permissionManager import PermissionManager
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from Services.permissionService import permissionService
class TestPermissionService:

    @pytest.fixture
    def permission_service(self):
        # Mock DAL object
        db_file = 'test_permissions.db'
        dal = MagicMock()
        ps = permissionService(db_file)
        ps.dal = dal
        return ps

    def test_create_permission_success(self, permission_service):
        # Arrange
        action = Action.DELETE
        resource = Resource.DATABASE
        effect = Effect.DENY
        
        permission_service.is_permission_exists = MagicMock(return_value=False)
        permission_service.dal.create_permission = MagicMock(return_value={
            'action': action.value, 
            'resource': resource.value, 
            'effect': effect.value
        })

        # Act
        result = permission_service.create_permission(action, resource, effect)

        # Assert
        assert result == {'action': action.value, 'resource': resource.value, 'effect': effect.value}
        permission_service.is_permission_exists.assert_called_once_with(action, resource, effect)
        permission_service.dal.create_permission.assert_called_once_with({
            'action': action.value,
            'resource': resource.value,
            'effect': effect.value
        })

    def test_create_permission_already_exists(self, permission_service):
        # Arrange
        action = Action.WRITE
        resource = Resource.DATABASE
        effect = Effect.DENY
        
        permission_service.is_permission_exists = MagicMock(return_value=True)

        # Act and Assert
        with pytest.raises(ValueError, match=f"Permission '{action}, {resource}, {effect}' already exists."):
            permission_service.create_permission(action, resource, effect)
        
        permission_service.is_permission_exists.assert_called_once_with(action, resource, effect)
        permission_service.dal.create_permission.assert_not_called()
