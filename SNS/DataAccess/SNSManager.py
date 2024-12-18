<<<<<<< HEAD
from collections import defaultdict
import json
from typing import List
from DB.NEW_KT_DB.DataAccess.ObjectManager import ObjectManager
from SNS.Model.SNSModel import Protocol, SNSTopicModel


class SNSTopicManager:

    def __init__(self, object_manager: ObjectManager):
        self.object_manager = object_manager
        self.object_manager.create_management_table(
            SNSTopicModel.get_object_name(), SNSTopicModel.table_schema, 'TEXT')

    def create_topic(self, sns_model: SNSTopicModel):
=======
import json
from DB.NEW_KT_DB.DataAccess.ObjectManager import ObjectManager
from SNS.Model.SNSModel import SNSTopicModel


class SNSTopicManager:
    """
    A class to manage SNS topics using ObjectManager.
    """

    def __init__(self, db_file: str):
        """
        Initialize the SNSTopicManager with an ObjectManager.

        Args:
            object_manager (ObjectManager): The ObjectManager instance for data operations.
        """
        self.object_manager = ObjectManager(db_file)
        self.object_manager.create_management_table(
            SNSTopicModel.get_object_name(), SNSTopicModel.table_structure, pk_column_data_type='TEXT')

    def create_topic(self, sns_model: SNSTopicModel):
        """
        Create a new SNS topic.

        Args:
            sns_model (SNSTopicModel): The SNSTopicModel instance to be created.
        """
>>>>>>> fe49bffeff811509c9dbc52c0399d1d6a288665e
        self.object_manager.save_in_memory(
            SNSTopicModel.get_object_name(), sns_model.to_sql())

    def delete_topic(self, topic_name: str) -> None:
<<<<<<< HEAD
=======
        """
        Delete an SNS topic by its name.

        Args:
            topic_name (str): The name of the topic to be deleted.
        """
>>>>>>> fe49bffeff811509c9dbc52c0399d1d6a288665e
        self.object_manager.delete_from_memory_by_pk(
            SNSTopicModel.get_object_name(), SNSTopicModel.pk_column, topic_name)

    def get_topic(self, topic_name: str) -> SNSTopicModel:
<<<<<<< HEAD
        sns_topic_list = self.object_manager.get_from_memory(
            SNSTopicModel.get_object_name(), '*', f'{SNSTopicModel.pk_column} =  "{topic_name}"')
        topic_name, subscribers = sns_topic_list[0]
        sns_topic = SNSTopicModel(topic_name)
        sns_topic.subscribers = defaultdict(list, json.loads(subscribers))
        return sns_topic

    def update_topic(self, sns_model: SNSTopicModel):
        self.object_manager.update_in_memory(
            SNSTopicModel.get_object_name(), sns_model.to_sql(), f'{sns_model.pk_column} = "{sns_model.pk_value}"')

    def is_exist_topic(self, topic_name: str):
=======
        """
        Retrieve an SNS topic by its name.

        Args:
            topic_name (str): The name of the topic to retrieve.

        Returns:
            SNSTopicModel: The retrieved SNS topic model.

        Raises:
            ValueError: If the topic is not found.
        """
        try:
            sns_topic_list = self.object_manager.get_from_memory(
                SNSTopicModel.get_object_name(), '*', f'{SNSTopicModel.pk_column} =  "{topic_name}"')
            topic_name, subscribers = sns_topic_list[0]
            return SNSTopicModel(topic_name, json.loads(subscribers))
        except:
            raise ValueError(f"Topic {topic_name} not found")

    def update_topic(self, sns_model: SNSTopicModel):
        """
        Update an existing SNS topic.

        Args:
            sns_model (SNSTopicModel): The updated SNSTopicModel instance.
        """
        sns_dict = sns_model.to_dict()
        updates = ', '.join([f"{key}='{json.dumps(value) if isinstance(
            value, dict) else value}'" for key, value in sns_dict.items()])
        self.object_manager.update_in_memory(
            SNSTopicModel.get_object_name(), updates, f'{sns_model.pk_column} = "{sns_model.pk_value}"')

    def is_topic_exist(self, topic_name: str) -> bool:
        """
        Check if a topic exists.

        Args:
            topic_name (str): The name of the topic to check.

        Returns:
            bool: True if the topic exists, False otherwise.
        """
>>>>>>> fe49bffeff811509c9dbc52c0399d1d6a288665e
        try:
            self.get_topic(topic_name)
            return True
        except:
            return False
