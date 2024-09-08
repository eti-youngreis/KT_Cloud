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
def test_can_describe_option_group(db_setup,create_option_group):
    res= db_setup.DescribeOptionGroups("mysql")#adjust parameters
    assert res["DescribeOptionGroupsResponse"]["ResponseMetadata"]["HTTPStatusCode"] == 200

def test_handle_invalid_engine_name(db_setup):
    with pytest.raises(ValueError):
        db_setup.DescribeOptionGroups("aaa")

def test_handle_too_short_max_records(db_setup):
    with pytest.raises(ValueError):
        db_setup.DescribeOptionGroups(max_records=10)

def test_handle_too_long_max_records(db_setup):
    with pytest.raises(ValueError):
        db_setup.DescribeOptionGroups(max_records=101)

# def test_handle_optionGroupName_supplied_with_EngineName(db_setup):
#     with pytest.raises(ValueError):#change the type of error?
#         db_setup.DescribeOptionGroups(engine_name="mysql",option_group_name="OptionGroupName")

# def test_handle_optionGroupName_supplied_with_MajorEngineVersion(db_setup):
#     with pytest.raises(ValueError):#change the type of error?
#         db_setup.DescribeOptionGroups(major_engine_version="5.7",option_group_name="OptionGroupName")


################################לשנות סטטוסים######################################
# def test_handle_option_group_name_not_found(db_setup):
#     with pytest.raises(ValueError):
#         db_setup.DescribeOptionGroups(option_group_name="invalide-name")


# def test_handle_no_option_group_with_specified_EngineName(db_setup):
#     with pytest.raises(ValueError):
#         db_setup.DescribeOptionGroups(engine_name="postgres")

# def test_handle_no_option_group_with_specified_MajorEngineVersion(db_setup):
#     with pytest.raises(ValueError):
#         db_setup.DescribeOptionGroups(major_engine_version="5.7")


