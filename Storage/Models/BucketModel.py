
class Bucket:
    def __init__(self, name: str, region, creationDate, policy, ACL, Tags, cors_configuration) -> None:
        self.name = name
        self.objects = {} #: Dict[str, Object] = {}
        self.region = region
        self.CreationDate = creationDate
        self.policy = policy
        self.ACL = ACL
        self.Tags = Tags
        self.cors_configuration = cors_configuration







