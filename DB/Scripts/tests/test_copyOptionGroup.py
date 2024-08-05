
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

def test_can_copy_option_group(db_setup,create_option_group):
    res= db_setup.CopyOptionGroup("my-option-group","copied-description","my-copied-option-group")
    assert res["CopyOptionGroupResponse"]["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert db_setup.DeleteOptionGroup("my-copied-option-group")["DeleteOptionGroupResponse"]["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert db_setup.DeleteOptionGroup("my-option-group")["DeleteOptionGroupResponse"]["ResponseMetadata"]["HTTPStatusCode"] == 200

    
def test_handle_TargetOptionGroupName_constraints_longer_than_255(db_setup,create_option_group):
    optionGroupName = "A" * 256
    with pytest.raises(ValueError):
        db_setup.CopyOptionGroup("my-option-group","copied-description",optionGroupName)
    
    assert db_setup.DeleteOptionGroup("my-option-group")["DeleteOptionGroupResponse"]["ResponseMetadata"]["HTTPStatusCode"] == 200

def test_handle__TargetOptionGroupName_constraints_shorter_than_1(db_setup):
    optionGroupName = ""
    with pytest.raises(ValueError):
         db_setup.CopyOptionGroup("my-option-group","copied-description",optionGroupName)
    
    # assert db_setup.DeleteOptionGroup("my-option-group")["DeleteOptionGroupResponse"]["ResponseMetadata"]["HTTPStatusCode"] == 200

def test_handle__TargetOptionGroupName_constraints_doesnt_start_with_a_letter(db_setup):
    optionGroupName = "1a"
    with pytest.raises(ValueError):
         db_setup.CopyOptionGroup("my-option-group","copied-description",optionGroupName)
    
    # assert db_setup.DeleteOptionGroup("my-option-group")["DeleteOptionGroupResponse"]["ResponseMetadata"]["HTTPStatusCode"] == 200

def test_handle__TargetOptionGroupName_constraints_doesnt_start_with_hyphen(db_setup):
    optionGroupName = "-myOptionGroup"
    with pytest.raises(ValueError):
          db_setup.CopyOptionGroup("my-option-group","copied-description",optionGroupName)
    
    # assert db_setup.DeleteOptionGroup("my-option-group")["DeleteOptionGroupResponse"]["ResponseMetadata"]["HTTPStatusCode"] == 200

def test_handle__TargetOptionGroupName_constraints_doesnt_contain_consecutive_hyphens(db_setup):
    optionGroupName = "myOption--Group"
    with pytest.raises(ValueError):
          db_setup.CopyOptionGroup("my-option-group","copied-description",optionGroupName)
    
    # assert db_setup.DeleteOptionGroup("my-option-group")["DeleteOptionGroupResponse"]["ResponseMetadata"]["HTTPStatusCode"] == 200

def test_handle__TargetOptionGroup_already_exists_fault(db_setup):

    db_setup.CreateOptionGroup("mysql","5.6","description","name-test",{"key": "value1"})
    res = db_setup.CopyOptionGroup("my-option-group","copied-description","name-test")

    assert res["Error"]["HTTPStatusCode"] == 400

    assert db_setup.DeleteOptionGroup("name-test")["DeleteOptionGroupResponse"]["ResponseMetadata"]["HTTPStatusCode"] == 200
    # assert db_setup.DeleteOptionGroup("my-option-group")["DeleteOptionGroupResponse"]["ResponseMetadata"]["HTTPStatusCode"] == 200        


def test_handle_CopyOptionGroup_handle_exceeded_fault(db_setup):
    for i in range(0,20):
        rr=db_setup.CreateOptionGroup("mysql","5.6","description",f"name-{i}-test",{"key": "value1"})

    res = db_setup.CopyOptionGroup("name-3-test","copied-description","copied-option-group")
    assert res["Error"]["HTTPStatusCode"] == 400

    for i in range(0,20):
        db_setup.DeleteOptionGroup(f"name-{i}-test")

def test_handle_source_option_group_name_cannot_be_found(db_setup):
    res = db_setup.CopyOptionGroup("invalide-name","copied-description","copied-option-group")
    assert res["Error"]["HTTPStatusCode"] == 404