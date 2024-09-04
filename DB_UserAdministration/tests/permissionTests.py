
import unittest
from unittest.mock import MagicMock, patch
from permissionController import permissionController
from permissionService import permissionService
from permissionModel import Action, Resource

class TestPermissionController(unittest.TestCase):

    @patch('permissionService.permissionService')
    def test_update_permission_success(self, mock_permission_service):
        mock_service = mock_permission_service.return_value
        permission_controller = permissionController(mock_service)
        
        # Mocking the update_permission function in the service
        mock_service.update_permission.return_value = True
        
        # Call the function to be tested
        result = permission_controller.update_permission(1, Action.WRITE, Resource.DATABASE)
        
        # Assert that the update was successful
        self.assertTrue(result)
        
    @patch('permissionService.permissionService')
    def test_update_permission_failure(self, mock_permission_service):
        mock_service = mock_permission_service.return_value
        permission_controller = permissionController(mock_service)
        
        # Mocking the update_permission function in the service to return False
        mock_service.update_permission.return_value = False
        
        # Call the function to be tested
        result = permission_controller.update_permission(2, Action.READ, Resource.BUCKET)
        
        # Assert that the update failed
        self.assertFalse(result)

# if __name__ == '__main__':
#     unittest.main()
