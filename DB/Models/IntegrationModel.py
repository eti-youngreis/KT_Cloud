class IntegrationModel:
    def __init__(self,**kwargs):
        self.integration_name = kwargs['integration_name']
        self.source_arn = kwargs['source_arn']
        self.target_arn = kwargs['target_arn']
        self.additional_encryption_context = 'additional_encryption_context' if 'additional_encryption_context' in kwargs else None
        self.data_filter = 'data_filter' if 'data_filter' in kwargs else None
        self.description = 'description' if 'description' in kwargs else None
        self.kms_key_id = 'kms_key_id' if 'kms_key_id' in kwargs else None
        self.tags = 'tags' if 'tags' in kwargs else None


    def to_dict(self)-> Dict:
        return {
            'integration_name': self.integration_name,
            'source_arn': self.source_arn,
            'target_arn':self.target_arn,
            'additional_encryption_context': self.additional_encryption_context,
            'data_filter': self.data_filter,
            'description': self.description,
            'kms_key_id': self.kms_key_id,
            'tags': self.tags 
        }
        