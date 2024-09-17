from SNS.DataAccess.SNSManager import SNSTopicManager
from SNS.Model.SNSModel import SNSTopicModel
from SNS.Service.SNSService import SNSTopicService


import pytest
from unittest.mock import Mock
from SNS.Service.SNSService import SNSTopicService
from Storage.NEW_KT_Storage.DataAccess.StorageManager import StorageManager

@pytest.fixture
def mock_sns_topic_manager():
    return Mock(SNSTopicManager)


@pytest.fixture
def sns_topic_service(mock_sns_topic_manager: Mock):
    mock_sns_topic_manager.is_exist_topic.return_value = False
    mock_sns_topic_manager.get_topic.return_value = SNSTopicModel('test_topic')
    service = SNSTopicService(mock_sns_topic_manager, StorageManager('test_directory'), 'test_service_directory')
    yield service
    service.delete()
    
def test_create_sns_topic(sns_topic_service: SNSTopicService):
    sns_topic_service.create_sns_topic('test_topic')
    assert sns_topic_service.get_sns_topic('test_topic').topic_name == 'test_topic'