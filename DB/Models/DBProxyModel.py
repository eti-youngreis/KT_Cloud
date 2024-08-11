
from typing import Dict
from datetime import datetime
class DBProxyModel:
    def __init__(self, **kwargs):
        self.auth = kwargs['auth']
        self.creation_date = datetime.now()
        self.updation_date = None
    

    def to_dict(self) -> Dict:
        return {
            'auth':self.auth,
            'creation_date':self.creation_date
        }

