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
    db_setup.CreateDBCluster(db_cluster_identifier = "my-cluster1", engine = "postgres", port = 1150, storage_type = "aurora")

def test_can_describe_DBClusters(db_setup, create_DBCluster):
    assert db_setup.DescribeDBClusters(db_cluster_identifier = "my-cluster1")
    # assert db_setup.DeleteDBCluster("my-cluster1")["DeleteDBClusterResponse"]["ResponseMetadata"]["HTTPStatusCode"] == 200

def test_can_describe_DBClusters_with_max_records(db_setup, create_DBCluster):
    assert db_setup.DescribeDBClusters(db_cluster_identifier = "my-cluster1", max_records = 58)

def test_handle_db_cluster_identifier_cannot_be_found(db_setup, create_DBCluster):
    with pytest.raises(ValueError):
        db_setup.DescribeDBClusters(db_cluster_identifier = "invalid-name")

def test_handle_too_short_max_records(db_setup):
    with pytest.raises(ValueError):
        db_setup.DescribeDBClusters(max_records=19)

def test_handle_too_long_max_records(db_setup):
    with pytest.raises(ValueError):
        db_setup.DescribeDBClusters(max_records=101)

