import os
import pytest

from dotenv import load_dotenv

from DB.NEW_KT_DB.DataAccess.ObjectManager import ObjectManager
from DB.NEW_KT_DB.Test.GeneralTests import is_file_exist, is_object_equal
from SNS.DataAccess.SNSManager import SNSTopicManager
from SNS.Model.SNSModel import Protocol, SNSTopicModel
from SNS.Service.SNSService import SNSTopicService
from SNS.Service.SNSService import SNSTopicService
from Storage.NEW_KT_Storage.DataAccess.StorageManager import StorageManager

load_dotenv()


@pytest.fixture
def sns_object_manager():
    return ObjectManager('sns_test_db.db')


@pytest.fixture
def sns_topic_manager(sns_object_manager: ObjectManager):
    return SNSTopicManager(sns_object_manager)


@pytest.fixture
def sns_topic_service(storage_manager: StorageManager, sns_topic_manager: SNSTopicManager):
    service = SNSTopicService(
        sns_topic_manager, storage_manager, 'test_service_directory')
    yield service


@pytest.fixture
def sns_topic(sns_topic_service: SNSTopicService, sns_topic_manager: SNSTopicManager):
    sns_topic_model = SNSTopicModel('test_topic')
    sns_topic_service.create(sns_topic_model.topic_name)
    yield sns_topic_model
    sns_topic_manager.delete_topic(sns_topic_model.topic_name)


@pytest.fixture
def storage_manager():
    storage_manager = StorageManager('test_directory')
    yield storage_manager
    storage_manager.delete_directory('test_directory')


def test_create_sns_topic_with_valid_name(sns_topic_service: SNSTopicService, sns_topic: SNSTopicModel, storage_manager: StorageManager):
    assert sns_topic_service.sns_manager.is_exist_topic(sns_topic.topic_name)
    is_file_exist(storage_manager, sns_topic_service.get_file_path(
        sns_topic.topic_name))


def test_create_sns_topic_with_existing_name(sns_topic: SNSTopicModel, sns_topic_service: SNSTopicService):
    with pytest.raises(ValueError):
        sns_topic_service.create(sns_topic.topic_name)


def test_create_sns_topic_with_empty_name(sns_topic_service: SNSTopicService):
    with pytest.raises(ValueError):
        sns_topic_service.create(None)


def test_delete_sns_topic_with_valid_name(sns_topic: SNSTopicModel, sns_topic_service: SNSTopicService, storage_manager: StorageManager):
    sns_topic_service.delete(sns_topic.topic_name)
    assert not sns_topic_service.sns_manager.is_exist_topic(
        sns_topic.topic_name)
    is_file_exist(storage_manager, sns_topic_service.get_file_path(
        sns_topic.topic_name))


def test_delete_sns_topic_with_non_existing_name(sns_topic_service: SNSTopicService):
    with pytest.raises(ValueError):
        sns_topic_service.delete('non_existing_topic')


def test_subscribe_to_sns_topic_with_valid_protocol_and_endpoint(sns_topic: SNSTopicModel, sns_topic_service: SNSTopicService, storage_manager: StorageManager):
    test_email = os.getenv('EMAIL_ADDRESS')
    sns_topic_service.subscribe(
        sns_topic.topic_name, Protocol.EMAIL.value, test_email)
    print("IDDDK" , sns_topic_service.get_subscribers(sns_topic.topic_name))
    assert test_email in sns_topic_service.get_subscribers(sns_topic.topic_name)[
        Protocol.EMAIL.value]
    is_file_exist(storage_manager, sns_topic_service.get_file_path(
        sns_topic.topic_name))


def test_subscribe_to_sns_topic_with_unsupported_protocol(sns_topic: SNSTopicModel, sns_topic_service: SNSTopicService):
    with pytest.raises(ValueError):
        sns_topic_service.subscribe(
            sns_topic.topic_name, Protocol.SMS, '1234567890')


def test_subscribe_to_sns_topic_with_invalid_protocol(sns_topic: SNSTopicModel, sns_topic_service: SNSTopicService):
    with pytest.raises(ValueError):
        sns_topic_service.subscribe(
            sns_topic.topic_name, 'invalid_protocol', 'invalid_endpoint')


def test_subscribe_to_sns_topic_with_invalid_endpoint(sns_topic: SNSTopicModel, sns_topic_service: SNSTopicService):
    with pytest.raises(ValueError):
        sns_topic_service.subscribe(
            sns_topic.topic_name, Protocol.EMAIL.value, 'invalid_endpoint')


def test_subscribe_to_non_existing_sns_topic(sns_topic_service: SNSTopicService):
    with pytest.raises(ValueError):
        sns_topic_service.subscribe(
            'non_existing_topic', Protocol.EMAIL.value, 'test@example.com')


def test_get_subscribers(sns_topic: SNSTopicModel, sns_topic_service: SNSTopicService):
    subscribers = {Protocol.EMAIL.value: [os.getenv('EMAIL_ADDRESS')]}
    sns_topic_service.subscribe(
        sns_topic.topic_name, Protocol.EMAIL.value, subscribers[Protocol.EMAIL.value][0])
    assert sns_topic_service.get_subscribers(
        sns_topic.topic_name) == subscribers


def test_get_subscribers_of_non_existing_sns_topic(sns_topic_service: SNSTopicService):
    with pytest.raises(ValueError):
        sns_topic_service.get_subscribers('non_existing_topic')


def test_unsubscribe_from_sns_topic_with_valid_protocol_and_endpoint(sns_topic: SNSTopicModel, sns_topic_service: SNSTopicService):
    test_email = os.getenv('EMAIL_ADDRESS')
    sns_topic_service.subscribe(
        sns_topic.topic_name, Protocol.EMAIL.value, test_email)
    sns_topic_service.unsubscribe(
        sns_topic.topic_name, Protocol.EMAIL.value, test_email)
    assert test_email not in sns_topic_service.get_subscribers(sns_topic.topic_name)[
        Protocol.EMAIL.value]


def test_unsubscribe_from_sns_topic_with_invalid_protocol(sns_topic: SNSTopicModel, sns_topic_service: SNSTopicService):
    with pytest.raises(ValueError):
        sns_topic_service.unsubscribe(
            sns_topic.topic_name, 'invalid_protocol', 'invalid_endpoint')


def test_unsubscribe_from_sns_topic_with_invalid_endpoint(sns_topic: SNSTopicModel, sns_topic_service: SNSTopicService):
    with pytest.raises(ValueError):
        sns_topic_service.unsubscribe(
            sns_topic.topic_name, Protocol.EMAIL.value, 'invalid_endpoint')


def test_unsubscribe_from_non_existing_sns_topic(sns_topic_service: SNSTopicService):
    with pytest.raises(ValueError):
        sns_topic_service.unsubscribe(
            'non_existing_topic', Protocol.EMAIL.value, os.getenv('EMAIL_ADDRESS'))


def test_unsubscribe_from_sns_topic_with_non_existing_endpoint(sns_topic: SNSTopicModel, sns_topic_service: SNSTopicService):
    with pytest.raises(ValueError):
        sns_topic_service.unsubscribe(
            sns_topic.topic_name, Protocol.EMAIL.value, 'test2@example.com')


def test_get(sns_topic: SNSTopicModel, sns_topic_service: SNSTopicService):
    assert is_object_equal(sns_topic_service.get(
        sns_topic.topic_name), sns_topic)


def test_get_with_non_existing_sns_topic(sns_topic_service: SNSTopicService):
    with pytest.raises(ValueError):
        sns_topic_service.get('non_existing_topic')


def test_notificate_subscribers(sns_topic: SNSTopicModel, sns_topic_service: SNSTopicService):

    test_email1 = os.getenv('USER_EMAIL')
    test_email2 = os.getenv('USER_EMAIL2')
    print(test_email1, test_email2)
    sns_topic_service.subscribe(
        sns_topic.topic_name, Protocol.EMAIL.value, test_email1)
    sns_topic_service.subscribe(
        sns_topic.topic_name, Protocol.EMAIL.value, test_email2)
    sns_topic_service._notificate_subscribers(
        sns_topic.topic_name, 'Test message')


def test_notificate_subscribers_with_non_existing_topic(sns_topic_service: SNSTopicService):
    with pytest.raises(ValueError):
        sns_topic_service._notificate_subscribers(
            'non_existing_topic', 'Test message')


def test_send_email(sns_topic_service: SNSTopicService):

    test_email = os.getenv('USER_EMAIL')
    test_email2 = os.getenv('USER_EMAIL2')

    sns_topic_service._send_email(
        [test_email, test_email2], 'Test message')


