import unittest
from unittest.mock import MagicMock, patch
from datetime import datetime
import os
import shutil
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..")))

from DB.NEW_KT_DB.DataAccess.DBSnapshotNaiveManager import DBSnapshotNaiveManager
from DB.NEW_KT_DB.Service.Classes.DBSnapshotNaiveService import DBSnapshotNaiveService

class TestDBSnapshotService(unittest.TestCase):
    def setUp(self):
        self.dal_mock = MagicMock(spec=DBSnapshotNaiveManager)
        self.service = DBSnapshotNaiveService(self.dal_mock)

    @patch('os.getlogin', return_value='test_user')
    @patch('shutil.copytree')
    @patch('DB.NEW_KT_DB.Service.Classes.DBSnapshotNaiveService.DBSnapshotNaiveService.describe', return_value=MagicMock(BASE_PATH='path', endpoint='endpoint'))
    # def test_create(self, describe_mock, copytree_mock, getlogin_mock):
    def test_create(self, describe_mock, copytree_mock, getlogin_mock):
        db_snapshot_identifier = 'test-snapshot-id'
        db_instance_identifier = 'test-db-id'
        description = 'Test snapshot'
        progress = '50%'
        
        with patch('DB.NEW_KT_DB.Models.DBSnapshotNaiveModel.SnapshotNaive') as snapshot_mock:
            snapshot_instance = snapshot_mock.return_value
            self.dal_mock.createInMemoryDBSnapshot.return_value = True
            
            result = self.service.create(db_snapshot_identifier, db_instance_identifier, description, progress)
            
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

    # @patch('os.path.exists', return_value=True)
    # @patch('os.remove')
    # def test_delete(self, remove_mock, exists_mock):
    #     snapshot_name = 'test-snapshot'
        
    #     self.dal_mock.deleteInMemoryDBSnapshot.return_value = True
        
    #     result = self.service.delete(snapshot_name)
        
    #     exists_mock.assert_called_once_with(f"../snapshot/{snapshot_name}.db")
    #     remove_mock.assert_called_once_with(f"../snapshot/{snapshot_name}.db")
    #     self.dal_mock.deleteInMemoryDBSnapshot.assert_called_once()
    #     self.assertTrue(result)
    
    # @patch('DB.NEW_KT_DB.DataAccess.DBSnapshotManagerNaive.DBSnapshotManagerNaive.describeDBSnapshot', return_value=MagicMock())

    # def test_describe(self, describe_mock):
    #     db_instance_identifier = 'test-db-id'
    #     result = self.service.describe(db_instance_identifier)
        
    #     describe_mock.assert_called_once_with(db_instance_identifier)
    #     self.assertEqual(result, {})

    # @patch('DB.NEW_KT_DB.Models.DBSnapshotModelNaive', return_value=MagicMock(to_dict=MagicMock(return_value={})))
    # def test_modify(self, snapshot_mock):
    #     owner_alias = 'new_owner'
    #     status = 'active'
    #     description = 'Updated description'
    #     progress = '75%'
        
    #     self.service.modify(owner_alias=owner_alias, status=status, description=description, progress=progress)
        
    #     self.assertEqual(self.service.owner_alias, owner_alias)
    #     self.assertEqual(self.service.status, status)
    #     self.assertEqual(self.service.description, description)
    #     self.assertEqual(self.service.progress, progress)

    # def test_get(self):
    #     self.service.db_snapshot = None
    #     result = self.service.get()
    #     self.assertIsNone(result)
        
    #     self.service.db_snapshot = MagicMock(to_dict=MagicMock(return_value={'key': 'value'}))
    #     result = self.service.get()
    #     self.assertEqual(result, {'key': 'value'})

if __name__ == '__main__': 
    unittest.main()
