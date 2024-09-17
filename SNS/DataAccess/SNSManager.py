from DB.NEW_KT_DB.DataAccess.ObjectManager import ObjectManager
from SNS.Model.SNSModel import SNSTopicModel


class SNSTopicManager:

    def __init__(self, object_manager: ObjectManager):
        self.object_manager = object_manager

    def create_topic(self, sns_model: SNSTopicModel):
        self.object_manager.save_in_memory(sns_model.to_sql())

    def get_topic(self, topic_name: str) -> SNSTopicModel:
        sns_topic_dict = self.object_manager.get_object_from_management_table(
            SNSTopicModel.table_name, topic_name)
        sns_topic = SNSTopicModel(topic_name)
        sns_topic.subscribers = sns_topic_dict['subscribers']
        return sns_topic

    def update_topic(self, sns_model: SNSTopicModel):
        self.object_manager.update_in_memory(
            SNSTopicModel.table_name, sns_model.to_sql(), object_id=sns_model.topic_name)

    def is_exist_topic(self, topic_name: str):
        try:
            self.object_manager.get_object_from_management_table(
                SNSTopicModel.table_name, topic_name)
            return True
        except:
            return False
