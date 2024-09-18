import pytest
from KT_Cloud.Storage.NEW_KT_Storage.Validation.GeneralValidations import test_file_exists
from KT_Cloud.Storage.NEW_KT_Storage.Service.Classes import BucketObjectService


def test_create(option_group, capsys):
    """Test the create method."""
    # BucketObjectService.create("name"="DBClusterTest")
    # ...
    assert test_file_exists("DBClusterTest")