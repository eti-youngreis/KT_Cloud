from SNS.Model.SNSModel import Protocol, SNSTopicModel
from SNS.Service.SNSService import SNSTopicService


class SNSTopicController:
    
    def __init__(self, sns_service: SNSTopicService):
        self.sns_service = sns_service

    def create_topic(self, topic_name: str):
        self.sns_service.create_topic(topic_name)
    
    def delete_topic(self, topic_name: str):
        self.sns_service.delete_topic(topic_name)
    
    def subscribe(self, topic_name: str, protocol: Protocol, notification_endpoint: str):
        '''Subscribe to topic.'''
        self.sns_service.subscribe(topic_name, protocol, notification_endpoint)

    def unsubscribe(self, topic_name: str, protocol: Protocol, notification_endpoint: str):
        '''Unsubscribe from topic.'''
        self.sns_service.unsubscribe(topic_name, protocol, notification_endpoint)

    def get_sns_topic(self, topic_name: str) -> SNSTopicModel:
        '''Get an existing SNS topic.'''
        return self.sns_service.get(topic_name)