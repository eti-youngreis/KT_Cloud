import pytest
from unittest.mock import patch
from DB.Scripts.PyDB import *
from DB.Scripts.endpoint import Endpoint


@pytest.fixture
def setup_db():
    conn = sqlite3.connect(':memory:')  # שימוש במסד נתונים בזיכרון לבדיקות
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Management (
            name TEXT NOT NULL,
            id_number TEXT NOT NULL,
            metadata TEXT
        )
    ''')
    conn.commit()
    cursor.execute('''
        INSERT INTO Management (name, id_number, metadata) VALUES (?, ?, ?)
    ''', ("DBCluster", "cluster1", "{}"))
    cursor.execute('''
        INSERT INTO Management (name, id_number, metadata) VALUES (?, ?, ?)
    ''', ("DBInstance", "instance1", "{}"))
    cursor.execute('''
        INSERT INTO Management (name, id_number, metadata) VALUES (?, ?, ?)
    ''', ("DBInstance", "instance2", "{}"))
    conn.commit()
    yield conn
    conn.close()


def test_modify_not_exists_db_cluster_parameter_group(setup_db):
    # create_db_cluster_parameter_group('cluster_parameter_group_1', )
    with pytest.raises(ValueError):
        modify_db_cluster_parameter_group('cluster_parameter_group_1', '11')
    # assert 1 == 1


# from endpoint import Endpoint, modify_db_cluster_endpoint
# from db_instance import DBInstance
# from endpoint import Endpoint
# from parameter_group import ParameterGroup
# from db_cluster import DBCluster


# @pytest.fixture
# def endpoint():
#     return Endpoint(
#         cluster_identifier="cluster1",
#         endpoint_identifier="endpoint1",
#         endpoint_type="CUSTOM",
#         static_members=["member1"],
#         excluded_members=["member2"]
#     )


# def test_modify_endpoint(endpoint):
#     endpoint.modify(
#         endpoint_type="READ",
#         static_members=["member3"],
#         excluded_members=["member4"]
#     )
#     assert endpoint.endpoint_type == "READ"
#     assert endpoint.static_members == ["member3"]
#     assert endpoint.excluded_members == ["member4"]


# def test_get_metadata(endpoint):
#     expected_metadata = '{"endpoint_type": "CUSTOM", "static_members": ["member1"], "excluded_members": ["member2"]}'
#     assert endpoint.get_metadata() == expected_metadata


# def test_modify_with_empty_values(endpoint):
#     endpoint.modify(
#         endpoint_type="",
#         static_members=None,
#         excluded_members=None
#     )
#     assert endpoint.endpoint_type == "CUSTOM"
#     assert endpoint.static_members == ["member1"]
#     assert endpoint.excluded_members == ["member2"]


# def test_modify_with_long_identifiers(endpoint):
#     long_identifier = "x" * 1000
#     endpoint.modify(
#         endpoint_type=long_identifier,
#         static_members=[long_identifier],
#         excluded_members=[long_identifier]
#     )
#     assert endpoint.endpoint_type == long_identifier
#     assert endpoint.static_members == [long_identifier]
#     assert endpoint.excluded_members == [long_identifier]


# def test_modify_invalid_identifier(endpoint):
#     with pytest.raises(ValueError):
#         endpoint.modify(endpoint_type=None, static_members=None, excluded_members=None)


# @patch('Management.insert_into_management_table')
# def test_add_endpoint_integration(mock_insert):
#     cluster = DBCluster(cluster_identifier="cluster1")
#     endpoint_description = cluster.add_endpoint(
#         cluster_identifier="cluster1",
#         endpoint_identifier="endpoint1",
#         endpoint_type="READ",
#         static_members=["member1"],
#         excluded_members=["member2"]
#     )
#     assert endpoint_description['EndpointType'] == 'READ'
#     mock_insert.assert_called_once()


# def test_endpoint_lifecycle():
#     cluster = DBCluster(cluster_identifier="cluster1")
#     endpoint = cluster.add_endpoint(
#         cluster_identifier="cluster1",
#         endpoint_identifier="endpoint1",
#         endpoint_type="READ",
#         static_members=["member1"],
#         excluded_members=["member2"]
#     )
#     assert endpoint['EndpointType'] == 'READ'

#     cluster.modify_db_cluster_endpoint(
#         endpoint_identifier="endpoint1",
#         endpoint_type="WRITE"
#     )
#     endpoint = cluster.endpoints[0]
#     assert endpoint.endpoint_type == "WRITE"

#     cluster.delete_endpoint(endpoint)
#     assert endpoint not in cluster.endpoints
