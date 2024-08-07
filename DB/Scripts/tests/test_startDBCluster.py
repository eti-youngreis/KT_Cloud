# import pytest
# import sys
# import os
# sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
# from Sara_Lea_Marx_Functions import Sara_Lea_Marx_Functions

# @pytest.fixture()
# def create_DBCluster(db_setup):
#     db_setup.CreateDBCluster(db_cluster_identifier = "my-cluster1", engine = "postgres", port = 1150, storage_type = "aurora")

# def test_can_start_DBCluster(db_setup, create_DBCluster):
#     assert db_setup.StartDBCluster(db_cluster_identifier = "my-cluster1")

# def test_handle_db_cluster_identifier_cannot_be_found(db_setup, create_DBCluster):
#     with pytest.raises(ValueError):
#         db_setup.StartDBCluster(db_cluster_identifier = "invalid-name")

