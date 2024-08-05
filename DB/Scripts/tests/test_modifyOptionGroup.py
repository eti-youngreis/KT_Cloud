import pytest
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from Gili_Bolak_Functions import Gili_Bolak_Functions  

@pytest.fixture(scope='module')
def db_setup():
        db_file = 'object_management_db.db'
        library = Gili_Bolak_Functions(db_file)
        yield library
        # library.conn.close()

@pytest.fixture()
def create_option_group(db_setup):
    db_setup.CreateOptionGroup("mysql", "5.6", "description", "my-option-group")

def test_can_modify_option_group(db_setup):
    db_setup.CreateOptionGroup("mysql", "5.6", "description", "my-option-group-to-modify")
    res = db_setup.ModifyOptionGroup("my-option-group-to-modify")
    assert res["ModifyOptionGroupResponse"]["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert db_setup.DeleteOptionGroup("my-option-group-to-modify")["DeleteOptionGroupResponse"]["ResponseMetadata"]["HTTPStatusCode"] == 200

def test_handle__option_group_name_cannot_be_found(db_setup,create_option_group):
    res = db_setup.ModifyOptionGroup("invalid-name")
    assert res["Error"]["HTTPStatusCode"] == 404


def test_handle_option_group_name_none_value(db_setup,create_option_group):
    res = db_setup.ModifyOptionGroup(None)
    assert res["Error"]["HTTPStatusCode"] == 404


#add test with an unavailable option group
#change the returning value to the option group - and check that it was updated