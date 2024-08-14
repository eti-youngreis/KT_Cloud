# domain/versioning.py
class Version:
    def __init__(self, version_id: str):
        self.version_id = version_id
        
class Versioning:
    def __init__(self):
        self.versions:list[Version] = {}

    # def add_version(self, key: str, content: bytes):
    #     version_id = str(len(self.versions) + 1)
    #     self.versions[key] = Version(version_id, content)
    #     return version_id

    # def get_version(self, key: str, version_id: str) -> bytes:
    #     return self.versions[key].content if key in self.versions else None

