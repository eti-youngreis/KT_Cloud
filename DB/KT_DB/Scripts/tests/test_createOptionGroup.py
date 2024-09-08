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

def test_can_create_option_group(db_setup):
    res_create= db_setup.CreateOptionGroup("mysql", "5.6", "description", "name11")
    assert res_create["CreateOptionGroupResponse"]["ResponseMetadata"]["HTTPStatusCode"] == 200
    res_delete= db_setup.DeleteOptionGroup("name11")
    assert res_delete["DeleteOptionGroupResponse"]["ResponseMetadata"]["HTTPStatusCode"] == 200
  

def test_handle_invalid_engine_name(db_setup):
    with pytest.raises(ValueError):
        db_setup.CreateOptionGroup("aaa","5.6","description","name")

    
def test_handle_OptionGroupName_constraints_longer_than_255(db_setup):
    optionGroupName = "A" * 256
    with pytest.raises(ValueError):
        db_setup.CreateOptionGroup("mysql","5.6","description",optionGroupName)

def test_handle_OptionGroupName_constraints_shorter_than_1(db_setup):
    optionGroupName = ""
    with pytest.raises(ValueError):
        db_setup.CreateOptionGroup("mysql","5.6","description",optionGroupName)

def test_handle_OptionGroupName_constraints_doesnt_start_with_a_letter(db_setup):
    optionGroupName = "1a"
    with pytest.raises(ValueError):
        db_setup.CreateOptionGroup("mysql","5.6","description",optionGroupName)

def test_handle_OptionGroupName_constraints_doesnt_start_with_hyphen(db_setup):
    optionGroupName = "-myOptionGroup"
    with pytest.raises(ValueError):
        db_setup.CreateOptionGroup("mysql","5.6","description",optionGroupName)

def test_handle_OptionGroupName_constraints_doesnt_contain_consecutive_hyphens(db_setup):
    optionGroupName = "myOption--Group"
    with pytest.raises(ValueError):
        db_setup.CreateOptionGroup("mysql","5.6","description",optionGroupName)

def test_handle_OptionGroup_already_exists_fault(db_setup):

    db_setup.CreateOptionGroup("mysql","5.6","description","name-test",{"key": "value1"})
    res = db_setup.CreateOptionGroup("mysql","5.6","description","name-test",{"key": "value1"})
    assert res["Error"]["HTTPStatusCode"] == 400

        
def test_handle_OptionGroup_handle_exceeded_fault(db_setup):
    for i in range(1,19):
        db_setup.CreateOptionGroup("mysql","5.6","description",f"name-{i}-test",{"key": "value1"})

    res = db_setup.CreateOptionGroup("mysql","5.6","description","name-1-test",{"key": "value1"})
    assert res["Error"]["HTTPStatusCode"] == 400

    for i in range(1,19):
        db_setup.DeleteOptionGroup(f"name-{i}-test")
    

