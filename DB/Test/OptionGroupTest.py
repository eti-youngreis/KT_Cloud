import pytest
from DB import OptionGroup  

@pytest.fixture
def option_group():
    """Fixture to create an instance of OptionGroup."""
    return OptionGroup()

def test_create(option_group, capsys):
    """Test the create method."""
    option_group.create()
    captured = capsys.readouterr()
    assert "Creating OptionGroup" in captured.out

def test_delete(option_group, capsys):
    """Test the delete method."""
    option_group.delete()
    captured = capsys.readouterr()
    assert "Deleting OptionGroup" in captured.out

def test_describe(option_group, capsys):
    """Test the describe method."""
    option_group.describe()
    captured = capsys.readouterr()
    assert "Describing OptionGroup" in captured.out

def test_modify(option_group, capsys):
    """Test the modify method."""
    option_group.modify()
    captured = capsys.readouterr()
    assert "Modifying OptionGroup" in captured.out

def test_copy(option_group, capsys):
    """Test the copy method."""
    option_group.copy("test-id")
    captured = capsys.readouterr()
    assert "Copying OptionGroup with ID: test-id" in captured.out

def test_describe_option(option_group, capsys):
    """Test the describe_option method."""
    option_group.describe_option("test-id")
    captured = capsys.readouterr()
    assert "Describing option in OptionGroup with ID: test-id" in captured.out
