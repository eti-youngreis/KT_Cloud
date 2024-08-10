import pytest
from DB import LogFile  

@pytest.fixture
def log_file():
    """Fixture to create an instance of LogFile."""
    return LogFile()

def test_download(log_file, capsys):
    """Test the download method of LogFile."""
    log_file.download()
    captured = capsys.readouterr()
    assert "Downloading log file..." in captured.out

def test_describe(log_file, capsys):
    """Test the describe method of LogFile."""
    log_file.describe()
    captured = capsys.readouterr()
    assert "Describing log file..." in captured.out
