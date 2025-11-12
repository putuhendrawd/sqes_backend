import pytest
from unittest.mock import mock_open, patch
from sqes.services import config_loader

# This is a fake config.ini file that we'll "load"
FAKE_INI_CONTENT = """
[basic]
use_database = postgresql
spike_method = fast
cpu_number_used = 16

[postgresql]
host = localhost
user = testuser
pool_size = 32
"""

def test_load_config_parses_correctly(mocker):
    """
    Tests that the loader correctly parses strings and integers.
    """
    # 1. Mock 'open': When open() is called, pretend to read FAKE_INI_CONTENT
    mocker.patch("builtins.open", mock_open(read_data=FAKE_INI_CONTENT))
    
    # 2. Mock 'os.path.exists' to return True so it finds the "file"
    mocker.patch("os.path.exists", return_value=True)
    
    # 3. Run the function
    config = config_loader.load_config(section='postgresql')
    
    # 4. Assert the results
    assert config['host'] == 'localhost'
    assert config['user'] == 'testuser'
    # Check that it correctly converted pool_size to an int
    assert config['pool_size'] == 32
    assert isinstance(config['pool_size'], int)

def test_load_config_raises_file_not_found(mocker):
    """
    Tests that it raises an error if the file doesn't exist.
    """
    # Mock 'os.path.exists' to return False
    mocker.patch("os.path.exists", return_value=False)
    
    # Use pytest.raises to check that the correct error is thrown
    with pytest.raises(FileNotFoundError):
        config_loader.load_config(section='basic')