class CORSConfigurationModel:
    def __init__(self, cors_rules=None):
        # cors_rules should be a list of dictionaries representing CORS rules
        self.cors_rules = cors_rules if cors_rules else []

    def to_dict(self):
        return {
            "CORSRules": self.cors_rules
        }
