from SNS.Service.SNSService import SNSTopicService


import pytest


def test_create_sns_topic():
    sns_topic_service = SNSTopicService(sns_topic_manager, storage_manager, directory)