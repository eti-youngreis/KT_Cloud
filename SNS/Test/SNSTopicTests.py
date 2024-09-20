import os
import pytest


from DB.NEW_KT_DB.DataAccess.ObjectManager import ObjectManager
from DB.NEW_KT_DB.Test.GeneralTests import is_file_exist, is_object_equal
from SNS.DataAccess.SNSManager import SNSTopicManager
from SNS.Model.SNSModel import Protocol, SNSTopicModel
from SNS.Service.SNSService import SNSTopicService
from Storage.NEW_KT_Storage.DataAccess.StorageManager import StorageManager


@pytest.fixture
def sns_object_manager():
    return ObjectManager('sns_test_db.db')


@pytest.fixture
def sns_topic_manager(sns_object_manager: ObjectManager):
    return SNSTopicManager(sns_object_manager)


@pytest.fixture
def storage_manager():
    test_directory = 'test_directory'
    storage_manager = StorageManager(test_directory)
    yield storage_manager
    storage_manager.delete_directory(test_directory)


@pytest.fixture
def sns_topic_service(storage_manager: StorageManager, sns_topic_manager: SNSTopicManager):
    service = SNSTopicService(
        sns_topic_manager, storage_manager, 'test_service_directory')
    yield service


@pytest.fixture
def test_sns_topic(sns_topic_service: SNSTopicService, sns_topic_manager: SNSTopicManager):
    sns_topic_model = SNSTopicModel('test_topic')
    sns_topic_service.create(sns_topic_model.topic_name)
    yield sns_topic_model
    sns_topic_manager.delete_topic(sns_topic_model.topic_name)


def test_create_sns_topic(sns_topic_service: SNSTopicService, test_sns_topic: SNSTopicModel, storage_manager: StorageManager):
    """
    Verify that creating a topic with a valid name succeeds.
    """
    assert sns_topic_service.sns_manager.is_topic_exist(test_sns_topic.topic_name)
    assert is_file_exist(
        storage_manager, sns_topic_service.get_file_path(test_sns_topic.topic_name))


@pytest.mark.parametrize('topic_name', [
    (''),  # empty topic name
    ('test_topic')  # existing topic name
])
def test_create_sns_topic_invalid_cases(sns_topic_service: SNSTopicService, test_sns_topic: SNSTopicModel, topic_name):
    """
    Test invalid cases for topic creation, including empty and existing names.
    """
    # Ensure sns_topic is created before running the test.
    with pytest.raises(ValueError):
        sns_topic_service.create(topic_name)


@pytest.mark.parametrize('topic_name', [
    ('non_existing_topic')
])
def test_delete_sns_topic_invalid_cases(sns_topic_service: SNSTopicService, test_sns_topic: SNSTopicModel, topic_name):
    """
    Test invalid cases of topic creation - empty or existing name.
    """
    with pytest.raises(ValueError):
        sns_topic_service.delete(topic_name)


def test_delete_sns_topic(test_sns_topic: SNSTopicModel, sns_topic_service: SNSTopicService, storage_manager: StorageManager):
    """
    Verify that deleting an existing topic works as expected.
    """
    sns_topic_service.delete(test_sns_topic.topic_name)
    assert not sns_topic_service.sns_manager.is_topic_exist(
        test_sns_topic.topic_name)
    assert not is_file_exist(
        storage_manager, sns_topic_service.get_file_path(test_sns_topic.topic_name))


@pytest.mark.parametrize('topic_name, protocol, endpoint', [
    ('non_existing_topic', Protocol.EMAIL.value,
     'test@example.com'),  # invalid topic name
    ('test_topic', Protocol.EMAIL.value, 'invalid_endpoint'),  # invalid endpoint
    ('test_topic', 'invalid protocol', 'test@example.com')  # invalid protocol
])
def test_subscribe_invalid_cases(sns_topic_service: SNSTopicService, topic_name, protocol, endpoint):
    """
    Test subscribing with invalid topic name, protocols or endpoints.
    """
    with pytest.raises(ValueError):
        sns_topic_service.subscribe(topic_name, protocol, endpoint)


def test_subscribe_to_sns_topic(test_sns_topic: SNSTopicModel, sns_topic_service: SNSTopicService, storage_manager: StorageManager):
    """
    Verify that subscribing to a topic works as expected.
    """
    test_email = 'test@example.com'
    sns_topic_service.subscribe(
        test_sns_topic.topic_name, Protocol.EMAIL.value, test_email)
    assert test_email in sns_topic_service.get_subscribers(test_sns_topic.topic_name)[
        Protocol.EMAIL.value]
    assert is_file_exist(
        storage_manager, sns_topic_service.get_file_path(test_sns_topic.topic_name))


@pytest.mark.parametrize('topic_name, protocol, endpoint', [
    ('non_existing_topic', Protocol.EMAIL.value,
     'test@example.com'),  # invalid topic name
    ('test_topic', Protocol.EMAIL.value, 'invalid_endpoint'),  # invalid endpoint
    ('test_topic', 'invalid protocol', 'test@example.com'),  # invalid protocol
])
def test_unsubscribe_invalid_cases(sns_topic_service: SNSTopicService, topic_name, protocol, endpoint):
    """
    Test unsubscribing with invalid topic name, protocols or endpoints.
    """
    with pytest.raises(ValueError):
        sns_topic_service.unsubscribe(topic_name, protocol, endpoint)


def test_unsubscribe_from_sns_topic(test_sns_topic: SNSTopicModel, sns_topic_service: SNSTopicService):
    """
    Verify that unsubscribing from a topic works as expected.
    """
    test_email = 'test@example.com'
    sns_topic_service.subscribe(
        test_sns_topic.topic_name, Protocol.EMAIL.value, test_email)
    sns_topic_service.unsubscribe(
        test_sns_topic.topic_name, Protocol.EMAIL.value, test_email)
    assert test_email not in sns_topic_service.get_subscribers(test_sns_topic.topic_name)[
        Protocol.EMAIL.value]


def test_get_sns_topic(test_sns_topic: SNSTopicModel, sns_topic_service: SNSTopicService):
    """
    Verify retrieving an existing topic works.
    """
    assert is_object_equal(sns_topic_service.get(
        test_sns_topic.topic_name), test_sns_topic)


def test_notify_subscribers(test_sns_topic: SNSTopicModel, sns_topic_service: SNSTopicService):
    """
    Verify notify works for valid subscribers.
    """
    test_email1 = 'test@example.com'
    test_email2 = 'test2@example.com'
    sns_topic_service.subscribe(
        test_sns_topic.topic_name, Protocol.EMAIL.value, test_email1)
    sns_topic_service.subscribe(
        test_sns_topic.topic_name, Protocol.EMAIL.value, test_email2)
    sns_topic_service.notify_subscribers(
        test_sns_topic.topic_name, 'Test message')


def test_send_email(sns_topic_service: SNSTopicService):
    """
    Verify sending an email to multiple recipients works.
    """
    test_email = 'test@example.com'
    test_email2 = 'test2@example.com'
    sns_topic_service._send_email([test_email, test_email2], 'Test message')
