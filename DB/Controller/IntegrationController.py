from Service import IntegrationService


class IntegrationController:
    def __init__(self, service: IntegrationService):
        self.service = service

    def create_integration(self, **kwargs):
        return self.service.create(**kwargs)

    def delete_integration(self, **kwargs):
        return self.service.delete(**kwargs)

    def modify_integration(self, **kwargs):
        return self.service.modify(**kwargs)


    def describe_integrations(self, **kwargs):
        return self.service.describe
        
