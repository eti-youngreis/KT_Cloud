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

@pytest.fixture(scope='module')
def create_option_group(db_setup):
    db_setup.CreateOptionGroup("mysql", "5.6", "description", "my-option-group")


#to check some other things in the return value?
def test_can_describe_option_group_options(db_setup,create_option_group):
    res= db_setup.DescribeOptionGroupOptions("mysql")#adjust parameters
    assert res["DescribeOptionGroupOptionsResponse"]["ResponseMetadata"]["HTTPStatusCode"] == 200

def test_handle_invalid_engine_name(db_setup):
    with pytest.raises(ValueError):
        db_setup.DescribeOptionGroupOptions("aaa")

def test_handle_too_short_max_records(db_setup):
    with pytest.raises(ValueError):
        db_setup.DescribeOptionGroupOptions(engine_name = "mysql", max_records=10)

def test_handle_too_long_max_records(db_setup):
    with pytest.raises(ValueError):
        db_setup.DescribeOptionGroupOptions(engine_name = "mysql", max_records=101)

# def test_handle_missing_required_EngineName(db_setup):#change the error that is sent?
#     with pytest.raises(ValueError):
#         db_setup.DescribeOptionGroupOptions(max_records=25)