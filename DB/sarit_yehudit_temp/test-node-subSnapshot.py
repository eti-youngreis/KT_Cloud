import os
import pytest
from node_subSnapshot import Node_SubSnapshot

@pytest.fixture
def root_node():
    endpoint = "test_endpoint"
    if not os.path.exists(endpoint):
        os.mkdir(endpoint)
    return Node_SubSnapshot(parent=None, endpoint=endpoint)

def test_node_creation(root_node):
    assert root_node.id_snepshot == 1
    assert root_node.parent is None
    assert root_node.children == []

def test_create_child(root_node):
    child_node = root_node.create_child(endpoint="test_endpoint")
    assert child_node.parent == root_node
    assert len(root_node.children) == 1
    assert root_node.children[0] == child_node
