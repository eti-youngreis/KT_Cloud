import pytest

from DB.Scripts.PyDB import *


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

def test_modify_not_exists():
    with pytest.raises(ValueError):
        describe_db_cluster_parameters('1')

def test_describe_db_cluster_parameters(sample_db_cluster_parameter_groups, setup_db):
    result = describe_db_cluster_parameters(sample_db_cluster_parameter_groups[0].parameter_group_name,
                                            source='engine-default',
                                            parameter_groups_in_func=sample_db_cluster_parameter_groups)
    assert result['Parameters'][0]['ParameterName'] == 'backup_retention_period'

