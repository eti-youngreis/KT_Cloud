import pytest
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from Sara_Lea_Marx_Functions import Sara_Lea_Marx_Functions

@pytest.fixture(scope='module')
def db_setup():
        db_file = 'object_management_db.db'
        library = Sara_Lea_Marx_Functions(db_file)
        yield library
        # library.conn.close()

@pytest.fixture()
def create_DBCluster(db_setup):
    db_setup.CreateDBCluster(db_cluster_identifier = "my-cluster1", engine = "postgres")

def test_can_modify_DBCluster(db_setup, create_DBCluster):
    assert db_setup.ModifyDBCluster(db_cluster_identifier = "my-cluster1")
    # assert db_setup.DeleteDBCluster("my-cluster1")["DeleteDBClusterResponse"]["ResponseMetadata"]["HTTPStatusCode"] == 200

def test_handle_db_cluster_identifier_cannot_be_found(db_setup, create_DBCluster):
    with pytest.raises(ValueError):
        db_setup.ModifyOptionGroup(db_cluster_identifier = "invalid-name")

def test_invalid_port_parameter_small(db_setup, create_DBCluster):
    with pytest.raises(ValueError):
        db_setup.ModifyOptionGroup(db_cluster_identifier = "my-cluster1", port = 1149)

def test_invalid_port_parameter_large(db_setup, create_DBCluster):
    with pytest.raises(ValueError):
        db_setup.ModifyOptionGroup(db_cluster_identifier = "my-cluster1", port = 65536)

# add tests to parameters: 
# db_cluster_parameter_group_name, engine_version 

