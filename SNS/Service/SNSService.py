from email.mime.text import MIMEText
import json
import os
from dotenv import load_dotenv
import smtplib
from typing import Dict, List
from SNS.DataAccess.SNSManager import SNSTopicManager
from SNS.Model.SNSModel import Protocol, SNSTopicModel
from SNS.Validation.SNSValidation import validate_endpoint_exist, validate_protocol, validate_topic_name, validate_topic_name_exist, validate_endpoint
from Storage.NEW_KT_Storage.DataAccess.StorageManager import StorageManager


class SNSTopicService:
    def __init__(self, sns_manager: SNSTopicManager, storage_manager: StorageManager, directory: str):
        self.sns_manager = sns_manager
        self.storage_manager = storage_manager
        self.directory = directory
        if not self.storage_manager.is_directory_exist(directory):
            self.storage_manager.create_directory(directory)

    def get_file_path(self, topic_name: str):
        return os.path.join(self.directory, topic_name + '.json')

    def create(self, topic_name: str):
        '''Create a new SNS topic.'''
        validate_topic_name(self.sns_manager, topic_name)

        sns_topic = SNSTopicModel(topic_name)

        self.sns_manager.create_topic(sns_topic)

        self.storage_manager.create_file(
            file_path=self.get_file_path(topic_name),
            content=json.dumps(sns_topic.to_dict())
        )

    def delete(self, topic_name: str):
        '''Delete an existing SNS topic.'''
        validate_topic_name_exist(self.sns_manager, topic_name)

        self.sns_manager.delete_topic(topic_name)

        self.storage_manager.delete_file(self.get_file_path(topic_name))

    def subscribe(self, topic_name: str, protocol: Protocol, endpoint: str):
        '''Subscribe to an SNS topic.'''

        # validations
        validate_protocol(protocol)
        validate_endpoint(protocol, endpoint)
        validate_topic_name_exist(self.sns_manager, topic_name)

        # add subscriber to sns topic
        sns_topic = self.get(topic_name)

        if protocol not in sns_topic.subscribers:
            sns_topic.subscribers[protocol] = []

        sns_topic.subscribers[protocol].append(endpoint)

        # update physical object
        self.storage_manager.delete_file(self.get_file_path(topic_name))
        self.storage_manager.create_file(
            file_path=self.get_file_path(topic_name),
            content=json.dumps(sns_topic.to_dict())
        )
        self.sns_manager.update_topic(sns_topic)

    def unsubscribe(self, topic_name: str, protocol: Protocol, endpoint: str):
        '''Unsubscribe from an SNS topic.'''
        # validations
        validate_protocol(protocol)
        validate_endpoint_exist(
            self.sns_manager, topic_name, protocol, endpoint)
        validate_topic_name_exist(self.sns_manager, topic_name)

        # remove subscriber from sns topic
        sns_topic = self.get(topic_name)
        try:
            sns_topic.subscribers[protocol].remove(endpoint)
        except ValueError:
            raise ValueError(
                f"Endpoint {endpoint} is not subscribed to topic {topic_name}")

        # update physical object
        self.storage_manager.delete_file(self.get_file_path(topic_name))
        self.storage_manager.create_file(
            file_path=self.get_file_path(topic_name),
            content=json.dumps(sns_topic.to_dict())
        )

        self.sns_manager.update_topic(sns_topic)

    def get_subscribers(self, topic_name: str) -> Dict[Protocol, List[str]]:
        '''Get subscribers of an existing SNS topic.'''

        validate_topic_name_exist(self.sns_manager, topic_name)
        return self.get(topic_name).subscribers

    def get(self, topic_name: str) -> SNSTopicModel:
        '''Get an existing SNS topic.'''

        validate_topic_name_exist(self.sns_manager, topic_name)
        return self.sns_manager.get_topic(topic_name)

    def notify(self, topic_name: str, message: str):
        '''Notify subscribers of an SNS topic.'''

        validate_topic_name_exist(self.sns_manager, topic_name)
        subscribers = self.get_subscribers(topic_name)
        for protocol, endpoints in subscribers.items():
            if len(endpoints) > 0:
                SNSTopicService.notification_functions[protocol](
                    self, endpoints, message)

    def _send_email(self, endpoints: List[str], message: str):
        '''Send an email to subscribers of an SNS topic.'''

        
        load_dotenv()

        for endpoint in endpoints:
            try:
                # Create a MIMEText object for the email content
                email_content = MIMEText(message)

                # Set up the email headers
                email_content['Subject'] = 'KT SNS Notification'
                email_content['To'] = endpoint

                # Connect to an SMTP server and send the email
                with smtplib.SMTP('smtp.gmail.com', 587) as server:
                    server.starttls()
                    server.login(os.getenv('EMAIL_ADDRESS'),
                                 os.getenv('APP_PASSWORD'))
                    server.send_message(email_content)

                print(f"Email sent successfully to {endpoint}")
            except Exception as e:
                print(f"Failed to send email to {endpoint}: {str(e)}")

    notification_functions = {
        Protocol.EMAIL.value: _send_email,
    }
