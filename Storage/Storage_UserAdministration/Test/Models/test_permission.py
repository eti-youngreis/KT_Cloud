import pytest
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'Models')))
from PermissionModel import Permission, Action, Resource, Effect


def test_get_permission_by_id_valid_id():
    permission_id = 2
    expected_permission = {
        "action": "read",
        "resource": "bucket",
        "effect": "allow"
    }
    
    result = Permission.get_permission_by_id(permission_id)
    assert result == expected_permission

def test_get_permission_by_id_invalid_id():
    invalid_permission_id = 999
    with pytest.raises(KeyError):
        Permission.get_permission_by_id(invalid_permission_id)


def test_get_id_by_permission_valid_details():
    action = Action.READ
    resource = Resource.BUCKET
    effect = Effect.DENY
    
    result = Permission.get_id_by_permission(action,resource,effect)
    
    assert result == 1     

def test_get_id_by_permission_invalid_details():
    invalid_action = "invalid_action"  
    invalid_resource = "invalid_resource"
    invalid_effect = "invalid_effect"
    
    with pytest.raises(ValueError):
        Permission.get_id_by_permission(
            Action(invalid_action),
            Resource(invalid_resource),
            Effect(invalid_effect)
        )

def test_permission_values():
    for permission_id, (action, resource, effect) in Permission._permissions.items():
        assert action in Action
        assert resource in Resource
        assert effect in Effect

