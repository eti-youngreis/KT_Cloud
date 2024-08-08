import os
import sys

import pytest
from unittest.mock import patch
# sys.path.append(os.path.abspath(os.path.join(
# os.path.dirname(__file__), '../Scripts')))
from DB.Scripts.PyDB import *
from DB.Scripts.endpoint import Endpoint


@pytest.fixture
def setup_db():
    conn = sqlite3.connect(':memory:')  # שימוש במסד נתונים בזיכרון לבדיקות
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Management (
            class_name TEXT NOT NULL,
            object_id TEXT NOT NULL,
            metadata TEXT
        )
    ''')
    conn.commit()
    cursor.execute('''
        INSERT INTO Management (class_name, object_id, metadata) VALUES (?, ?, ?)
    ''', ("DBClusterParameterGroup", "db_cluster_parameter_group_2", "{}"))
    cursor.execute('''
        INSERT INTO Management (class_name, object_id, metadata) VALUES (?, ?, ?)
    ''', ("DBInstance", "instance1", "{}"))
    cursor.execute('''
        INSERT INTO Management (class_name, object_id, metadata) VALUES (?, ?, ?)
    ''', ("DBInstance", "instance2", "{}"))
    conn.commit()
    yield conn
    conn.close()


@pytest.fixture
def sample_db_cluster_parameter_groups(setup_db):
    db_cluster_parameter_group = DBClusterParameterGroup('db_cluster_parameter_group_1', 'mysql8.0', 'sample')
    db_cluster_parameter_group.save_to_db(setup_db)
    # insert_into_management_table("DBClusterParameterGroup",db_cluster_parameter_group.parameter_group_name,db_cluster_parameter_group.)
    return [db_cluster_parameter_group]


def test_modify_not_exists_db_cluster_parameter_group():
    # create_db_cluster_parameter_group('cluster_parameter_group_1', )
    with pytest.raises(ValueError):
        modify_db_cluster_parameter_group('cluster_parameter_group_1', '11')
    # assert 1 == 1


def test_modify_db_cluster_parameter_group_parameter_does_not_exist(sample_db_cluster_parameter_groups, setup_db):
    conn = setup_db
    cursor = conn.cursor()
    # create_db_cluster_parameter_group('db_cluster_parameter_group_1', 'mysql8.0', 'sample')
    cursor.execute(
        'SELECT * FROM Management WHERE class_name = ? AND object_id = ?',
        ("DBClusterParameterGroup", "db_cluster_parameter_group_1"))
    result = cursor.fetchone()
    metadata_before = result[2]
    metadata_dict = json.loads(metadata_before)
    # print(metadata_dict['parameters'])
    Parameters = [
        {
            'ParameterName': 'max_connections',
            'ParameterValue': '150',
            'Description': 'Maximum number of connections',
            'Source': 'user',
            'ApplyType': 'dynamic',
            'DataType': 'integer',
            'AllowedValues': '1-10000',
            'IsModifiable': True,
            'MinimumEngineVersion': '10.1',
            'ApplyMethod': 'immediate',
            'SupportedEngineModes': ['provisioned']
        }
    ]

    modify_db_cluster_parameter_group('db_cluster_parameter_group_1', parameters=Parameters, conn=conn,
                                      parameter_groups_in_func=sample_db_cluster_parameter_groups)
    cursor.execute(
        'SELECT * FROM Management WHERE class_name = ? AND object_id = ?',
        ("DBClusterParameterGroup", "db_cluster_parameter_group_1"))
    result = cursor.fetchone()
    metadata_after = result[2]
    assert metadata_after == metadata_before
    # metadata_dict = json.loads(metadata_after)


def test_modify_db_cluster_parameter_group(sample_db_cluster_parameter_groups, setup_db):
    conn = setup_db
    cursor = conn.cursor()
    # create_db_cluster_parameter_group('db_cluster_parameter_group_1', 'mysql8.0', 'sample')
    cursor.execute(
        'SELECT * FROM Management WHERE class_name = ? AND object_id = ?',
        ("DBClusterParameterGroup", "db_cluster_parameter_group_1"))
    result = cursor.fetchone()
    metadata_before = result[2]
    metadata_dict = json.loads(metadata_before)
    # print(metadata_dict['parameters'])
    Parameters = [
        {
            'ParameterName': 'backup_retention_period',
            'ParameterValue': 9,
            'Description': 'Maximum number of connections',
            'Source': 'user',
            'ApplyType': 'dynamic',
            'DataType': 'integer',
            'AllowedValues': '1-10000',
            'IsModifiable': True,
            'MinimumEngineVersion': '10.1',
            'ApplyMethod': 'immediate',
            'SupportedEngineModes': ['provisioned']
        }
    ]

    modify_db_cluster_parameter_group('db_cluster_parameter_group_1', parameters=Parameters, conn=conn,
                                      parameter_groups_in_func=sample_db_cluster_parameter_groups)
    cursor.execute(
        'SELECT * FROM Management WHERE class_name = ? AND object_id = ?',
        ("DBClusterParameterGroup", "db_cluster_parameter_group_1"))
    result = cursor.fetchone()
    metadata_after = result[2]
    metadata_after_dict = json.loads(metadata_after)
    print(metadata_after_dict)
    assert metadata_after_dict['parameters'][0]['ParameterValue'] == 9


def test_describe_db_cluster_parameters(sample_db_cluster_parameter_groups, setup_db):
    with pytest.raises(ValueError):
        describe_db_cluster_parameters('1')


def test_describe_db_cluster_parameters(sample_db_cluster_parameter_groups, setup_db):
    result = describe_db_cluster_parameters(sample_db_cluster_parameter_groups[0].parameter_group_name,
                                            source='engine-default',
                                            parameter_groups_in_func=sample_db_cluster_parameter_groups)
    assert result['Parameters'][0]['ParameterName'] == 'backup_retention_period'
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
