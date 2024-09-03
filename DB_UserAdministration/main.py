import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from DB_UserAdministration.Controllers.PolicyController import PolicyController
from DB_UserAdministration.DataAccess.PolicyManager import PolicyManager
from DB_UserAdministration.Services.PolicyService import PolicyService
from DB_UserAdministration.Models.permissionModel import (
    Action,
    Effect,
    Permission,
    Resource,
)
from DB_UserAdministration.Models.PolicyModel import Policy

if __name__ == "__main__":
    manager = PolicyManager(
        "./DB_UserAdministration/DataAccess/data_access_architecture_test.db"
    )
    service = PolicyService(manager)
    controller = PolicyController(service)

    # controller.create_policy(
    #     Policy(
    #         "policy1",
    #         [
    #             Permission(Action.DELETE, Resource.BUCKET, Effect.ALLOW),
    #             Permission(Action.EXECUTE, Resource.DATABASE, Effect.DENY),
    #         ],
    #     )
    # )

    
    # print(controller.list_policies())
    policy1 = controller.get_policy('policy1')
    policy1.permissions = []
    policy1.add_permission(Permission(Action.READ, Resource.BUCKET, Effect.ALLOW))
    controller.subscribe(policy1)
    policy1 = controller.get_policy('policy1')
    print(policy1)
    # controller.delete_policy('policy1')
