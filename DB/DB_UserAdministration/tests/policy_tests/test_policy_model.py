import os
import sys

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
sys.path.insert(0, project_root)

from DB_UserAdministration.Models.PolicyModel import Policy


def test_policy_init():
    policy_test = Policy('test_policy')
    assert policy_test.name == 'test_policy' and policy_test.permissions == []