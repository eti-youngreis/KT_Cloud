
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from Models.AclModel import Acl
from Service.Classes.AclService import AclService

class AclController:
    def __init__(self, service: AclService):
        self.service = service


    def create_acl_(self, **kwargs):
        self.service.create(**kwargs)


    def delete_acl(self):
        self.service.delete()



    def get_all_acl_objects(self):
           self.service.load_acl_objects()

    def get_acl(self):
        self.service.get()