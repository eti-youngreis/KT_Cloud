import pytest
from GeneralTests import test_file_exists
from Service import DBClusterService


def test_create(option_group, capsys):
    """Test the create method."""
    DBClusterService.create("name"="DBClusterTest")
    # ...
    assert test_file_exists("DBClusterTest")