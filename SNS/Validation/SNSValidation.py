from SNS.DataAccess.SNSManager import SNSTopicManager
from SNS.Model.SNSModel import Protocol
import re


def validate_topic_name(sns_manager: SNSTopicManager, topic_name: str):
    '''Validate topic name.'''
    if not topic_name:
        raise ValueError('Topic name cannot be empty.')
<<<<<<< HEAD
    if sns_manager.is_exist_topic(topic_name):
=======
    if sns_manager.is_topic_exist(topic_name):
>>>>>>> fe49bffeff811509c9dbc52c0399d1d6a288665e
        raise ValueError('Topic already exists.')


def validate_topic_name_exist(sns_manager: SNSTopicManager, topic_name: str):
    '''Validate topic name does not exist.'''
    if not topic_name:
        raise ValueError('Topic name cannot be empty.')
<<<<<<< HEAD
    if not sns_manager.is_exist_topic(topic_name):
=======
    if not sns_manager.is_topic_exist(topic_name):
>>>>>>> fe49bffeff811509c9dbc52c0399d1d6a288665e
        raise ValueError('Topic does not exist.')


def validate_protocol(protocol: Protocol):
    '''Validate protocol.'''
<<<<<<< HEAD
    if protocol != Protocol.EMAIL:
=======
    if protocol != Protocol.EMAIL.value:
>>>>>>> fe49bffeff811509c9dbc52c0399d1d6a288665e
        raise ValueError('For now, KT-SNS supports only email protocol.')


def validate_endpoint(protocol: Protocol, endpoint: str):
    '''Validate endpoint.'''
    protocol_validations[protocol](endpoint)


<<<<<<< HEAD
def validate_email_address(email_address: str):
    if not re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', email_address):
=======
def validate_endpoint_exist(sns_manager: SNSTopicManager, topic_name: str, protocol: Protocol, endpoint: str):
    '''Validate endpoint exists.'''
    subscribers = sns_manager.get_topic(topic_name).subscribers
    if protocol not in subscribers or endpoint not in subscribers[protocol]:
        raise ValueError('Endpoint does not exist.')


def validate_email_address(email_address: str):
    if not re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', email_address):
>>>>>>> fe49bffeff811509c9dbc52c0399d1d6a288665e
        raise ValueError('Invalid email address.')


protocol_validations = {
<<<<<<< HEAD
    Protocol.EMAIL: validate_email_address
=======
    Protocol.EMAIL.value: validate_email_address
>>>>>>> fe49bffeff811509c9dbc52c0399d1d6a288665e
}
