import pip
import pytest
from unittest.mock import patch
from DB.Scripts.PyDB import *
from DB.Scripts.db_instance import DBInstance
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
    ''', ("DBCluster", "cluster1", "{}"))
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
def sample_clusters():
    cluster = DBCluster(cluster_identifier="cluster1")
    instance1 = DBInstance(db_instance_identifier="instance1", cluster_id="cluster1")
    instance2 = DBInstance(db_instance_identifier="instance2", cluster_id="cluster1")
    cluster.instances = [instance1, instance2]
    return [cluster]


@pytest.fixture
def endpoint():
    return Endpoint(
        cluster_identifier="cluster1",
        endpoint_identifier="endpoint1",
        endpoint_type="CUSTOM",
        static_members=["member1"],
        excluded_members=["member2"]
    )


def test_modify_not_exists_db_cluster_endpoint():
    with pytest.raises(ValueError):
        modify_db_cluster_endpoint('1')


def test_modify_db_cluster_endpoint(sample_clusters, setup_db):
    conn = setup_db
    cursor = conn.cursor()
    clusters = sample_clusters
    # modify_db_cluster_endpoint("endpoint")
    create_db_cluster_endpoint("cluster1", "db-tamar.colcjtm9obot.rds.vast-data.com", 'READER', excluded_members=[],
                               conn=conn, clusters_in_func=clusters)
    cursor.execute(
        'SELECT * FROM Management WHERE class_name = ? AND object_id = ?',
        ("Endpoint", "db-tamar.colcjtm9obot.rds.vast-data.com"))
    result = cursor.fetchone()
    assert result is not None
    modify_db_cluster_endpoint("db-tamar.colcjtm9obot.rds.vast-data.com", endpoint_type='ANY', conn=conn,
                               clusters_in_func=clusters)

    cursor.execute(
        'SELECT * FROM Management WHERE class_name = ? AND object_id = ?',
        ("Endpoint", "db-tamar.colcjtm9obot.rds.vast-data.com"))
    result = cursor.fetchone()
    metadata = result[2]
    metadata_dict = json.loads(metadata)
    assert metadata_dict['endpoint_type'] == 'ANY'
    modify_db_cluster_endpoint("db-tamar.colcjtm9obot.rds.vast-data.com", static_members=['instance1'], conn=conn,
                               clusters_in_func=clusters)
    cursor.execute(
        'SELECT * FROM Management WHERE class_name = ? AND object_id = ?',
        ("Endpoint", "db-tamar.colcjtm9obot.rds.vast-data.com"))
    result = cursor.fetchone()
    metadata = result[2]
    metadata_dict = json.loads(metadata)
    assert metadata_dict['static_members'] == ['instance1']
    modify_db_cluster_endpoint("db-tamar.colcjtm9obot.rds.vast-data.com", excluded_members=['instance1'], conn=conn,
                               clusters_in_func=clusters)
    cursor.execute(
        'SELECT * FROM Management WHERE class_name = ? AND object_id = ?',
        ("Endpoint", "db-tamar.colcjtm9obot.rds.vast-data.com"))
    result = cursor.fetchone()
    metadata = result[2]
    metadata_dict = json.loads(metadata)
    assert metadata_dict['excluded_members'] == ['instance1']
    assert 'instance1' not in metadata_dict['static_members']


def test_modify_with_empty_values(endpoint):
    endpoint.modify(
        endpoint_type="",
        static_members=None,
        excluded_members=None
    )
    assert endpoint.endpoint_type == 'CUSTOM'
    assert endpoint.static_members == ["member1"]
    assert endpoint.excluded_members == ["member2"]


def test_modify_invalid_endpoint_type(sample_clusters, setup_db):
    conn = setup_db
    cursor = conn.cursor()
    clusters = sample_clusters
    # modify_db_cluster_endpoint("endpoint")
    create_db_cluster_endpoint("cluster1", "db-tamar.colcjtm9obot.rds.vast-data.com", 'READER', excluded_members=[],
                               conn=conn, clusters_in_func=clusters)
    with pytest.raises(ValueError):
        modify_db_cluster_endpoint('db-tamar.colcjtm9obot.rds.vast-data.com', 'hello')
