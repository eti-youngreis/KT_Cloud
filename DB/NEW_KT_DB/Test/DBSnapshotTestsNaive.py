# import pytest
# from unittest.mock import Mock
# from DB.NEW_KT_DB.DataAccess import DBSnapshotManager
# from Models import DBSnapshotModel
# from Service.Classes.DBClusterService import DBClusterService

# @pytest.fixture
# def mock_dal():
#     return Mock(spec=DBSnapshotManager)

# def test_create_snapshot(mock_dal):
#     service = DBClusterService(mock_dal)
#     db_instance_identifier = "test_instance"
#     description = "Test snapshot"
#     progress = "100%"

#     result = service.create(db_instance_identifier, description, progress)

#     assert result is not None
#     assert isinstance(result, DBSnapshotModel)

# def test_delete_snapshot(mock_dal):
#     service = DBClusterService(mock_dal)
#     snapshot_name = "test_snapshot"

#     result = service.delete(snapshot_name)

#     assert result is not None
#     # Add more assertions based on the expected behavior of the delete method

# def test_describe_snapshot(mock_dal):
#     service = DBClusterService(mock_dal)

#     result = service.describe()

#     assert result is not None
#     # Add more assertions based on the expected behavior of the describe method

# def test_modify_snapshot(mock_dal):
#     service = DBClusterService(mock_dal)
#     owner_alias = "test_owner"
#     status = "active"
#     description = "Modified snapshot"
#     progress = "50%"

#     service.modify(owner_alias, status, description, progress)

#     # Add assertions to check if the attributes were modified correctly

# def test_get_snapshot(mock_dal):
#     service = DBClusterService(mock_dal)

#     result = service.get()

#     assert result is not None
#     # Add more assertions based on the expected behavior of the get method



import unittest
from unittest.mock import MagicMock, patch
from datetime import datetime
import os
import shutil
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..","..")))

from DB.NEW_KT_DB.DataAccess.DBSnapshotManagerNaive import DBSnapshotManager
from Models import Snapshot
from Service import DBClusterService
from Validation import is_valid_db_instance_identifier, is_valid_db_snapshot_description, is_valid_progress


class TestDBClusterService(unittest.TestCase):
    def setUp(self):
        # Mocking DBSnapshotManager
        self.dal_mock = MagicMock(spec=DBSnapshotManager)
        self.service = DBClusterService(self.dal_mock)
    
    @patch('os.getlogin', return_value='test_user')
    @patch('shutil.copytree')
    @patch('DB.NEW_KT_DB.Service.Classes.DBInstanceService.describe', return_value=MagicMock(BASE_PATH='path', endpoint='endpoint'))
    def test_create(self, describe_mock, copytree_mock, getlogin_mock):
        db_instance_identifier = 'test-db-id'
        description = 'Test snapshot'
        progress = '50%'
        
        # Mocking Snapshot constructor
        with patch('Models.Snapshot') as snapshot_mock:
            snapshot_instance = snapshot_mock.return_value
            self.dal_mock.createInMemoryDBSnapshot.return_value = True
            
            result = self.service.create(db_instance_identifier, description, progress)
            
            # Assertions
            snapshot_mock.assert_called_once_with(
                db_instance_identifier=db_instance_identifier,
                creation_date=datetime.now(),
                owner_alias='test_user',
                description=description,
                progress=progress,
                url_snapshot=f"../snapshot/{db_instance_identifier}_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.db"
            )
            self.dal_mock.createInMemoryDBSnapshot.assert_called_once()
            self.assertTrue(result)

    @patch('os.path.exists', return_value=True)
    @patch('os.remove')
    def test_delete(self, remove_mock, exists_mock):
        snapshot_name = 'test-snapshot'
        
        self.dal_mock.deleteInMemoryDBSnapshot.return_value = True
        
        result = self.service.delete(snapshot_name)
        
        # Assertions
        exists_mock.assert_called_once_with(f"../snapshot/{snapshot_name}.db")
        remove_mock.assert_called_once_with(f"../snapshot/{snapshot_name}.db")
        self.dal_mock.deleteInMemoryDBSnapshot.assert_called_once()
        self.assertTrue(result)
    
    @patch('DB.NEW_KT_DB.DataAccess.DBSnapshotManager.describeDBSnapshot', return_value={})
    def test_describe(self, describe_mock):
        result = self.service.describe()
        
        # Assertions
        describe_mock.assert_called_once()
        self.assertEqual(result, {})

    @patch('Models.Snapshot', return_value=MagicMock(to_dict=MagicMock(return_value={})))
    def test_modify(self, snapshot_mock):
        owner_alias = 'new_owner'
        status = 'active'
        description = 'Updated description'
        progress = '75%'
        
        self.service.modify(owner_alias=owner_alias, status=status, description=description, progress=progress)
        
        # Assertions
        self.assertEqual(self.service.owner_alias, owner_alias)
        self.assertEqual(self.service.status, status)
        self.assertEqual(self.service.description, description)
        self.assertEqual(self.service.progress, progress)

    def test_get(self):
        # Test the get method when db_snapshot is None
        self.service.db_snapshot = None
        result = self.service.get()
        self.assertIsNone(result)
        
        # Test the get method when db_snapshot is set
        self.service.db_snapshot = MagicMock(to_dict=MagicMock(return_value={'key': 'value'}))
        result = self.service.get()
        self.assertEqual(result, {'key': 'value'})

if __name__ == '__main__':
    unittest.main()
