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

def test_can_create_valid_DBCluster(db_setup):
    assert db_setup.CreateDBCluster(db_cluster_identifier = "my-cluster1", engine = "postgres", port = 1150, storage_type = "aurora")
    # res_delete= db_setup.DeleteDBCluster("name11")
    # assert res_delete["DeleteDBClusterResponse"]["ResponseMetadata"]["HTTPStatusCode"] == 200

def test_handle_DBClusterIdentifier_already_exists_fault(db_setup):
    with pytest.raises(ValueError):
        db_setup.CreateDBCluster(db_cluster_identifier = "my-cluster1", engine = "postgres")
        db_setup.CreateDBCluster(db_cluster_identifier = "my-cluster1", engine = "postgres")

def test_invalid_DBClusterIdentifier_length_aurora(db_setup):
    with pytest.raises(ValueError):
        db_setup.CreateDBCluster(db_cluster_identifier = "a" * 64, engine = "aurora-mysql")

# def test_invalid_DBClusterIdentifier_length_multi_az(db_setup):
#     with pytest.raises(ValueError):
#         db_setup.CreateDBCluster(db_cluster_identifier = "a" * 53, engine = "mysql")

def test_invalid_DBClusterIdentifier_characters(db_setup):
    with pytest.raises(ValueError):
        db_setup.CreateDBCluster(db_cluster_identifier = "my_cluster1", engine = "aurora-postgresql")

def test_invalid_DBClusterIdentifier_start_char(db_setup):
    with pytest.raises(ValueError):
        db_setup.CreateDBCluster(db_cluster_identifier = "1my-cluster", engine = "aurora-mysql")

def test_invalid_DBClusterIdentifier_end_hyphen(db_setup):
    with pytest.raises(ValueError):
        db_setup.CreateDBCluster(db_cluster_identifier = "my-cluster-", engine = "aurora-mysql")

def test_invalid_DBClusterIdentifier_consecutive_hyphens(db_setup):
    with pytest.raises(ValueError):
        db_setup.CreateDBCluster(db_cluster_identifier = "my--cluster", engine = "aurora-mysql")

def test_valid_DBClusterIdentifier_edge_cases(db_setup):
    assert db_setup.CreateDBCluster(db_cluster_identifier = "my-cluster1", engine = "aurora-mysql")
    assert db_setup.CreateDBCluster(db_cluster_identifier = "a" * 63, engine = "aurora-mysql")
    assert db_setup.CreateDBCluster(db_cluster_identifier = "a" * 52, engine = "mysql")

def test_handle_invalid_engine(db_setup):
    with pytest.raises(ValueError):
        db_setup.CreateDBCluster(db_cluster_identifier = "my-cluster1", engine = "invalid-engine")

def test_handle_missing_required_engine(db_setup):  
    with pytest.raises(ValueError):
        db_setup.CreateDBCluster(db_cluster_identifier = "my-cluster1")

def test_invalid_port_parameter_small(db_setup):
    with pytest.raises(ValueError):
        db_setup.CreateDBCluster(db_cluster_identifier = "my-cluster1", port = 1149)

def test_invalid_port_parameter_large(db_setup):
    with pytest.raises(ValueError):
        db_setup.CreateDBCluster(db_cluster_identifier = "my-cluster1", port = 65536)

def test_invalid_storage_type_parameter(db_setup):
    with pytest.raises(ValueError):
        db_setup.CreateDBCluster(db_cluster_identifier = "my-cluster1", engine = "postgres", port = 1150, storage_type = "aurora2")


# add tests to parameters: 
# availability_zones, db_cluster_parameter_group_name, db_subnet_group_name, enable_cloudwatch_logs_exports, engine_version 
        