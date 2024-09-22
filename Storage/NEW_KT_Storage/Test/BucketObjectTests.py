import pytest
from GeneralTests import test_file_exists
from Service import BucketObjectService


def test_create(option_group, capsys):
    """Test the create method."""
    BucketObjectService.create("name"="DBClusterTest")
    # ...
    assert test_file_exists("DBClusterTest")