# tests/test_data_updater.py
import pytest
import pandas as pd
import requests
from unittest.mock import MagicMock, call
from sqlalchemy import text

# Import the module to test
# (If your folder is 'processing', change 'analysis' to 'processing')
from sqes.utils import sensor_updater 

# --- Fixtures: Mocked External Services ---

@pytest.fixture
def mock_requests_get(mocker):
    """Mocks the requests.get() call to return a fake HTML table."""
    FAKE_HTML = """
    <table>
      <thead>
        <tr><th>Station/Channel</th><th>Sensor Type</th></tr>
      </thead>
      <tbody>
        <tr><td>TEST 00 BHE</td><td>Streckeisen STS-2</td></tr>
        <tr><td>TEST 00 BHN</td><td>Streckeisen STS-2</td></tr>
        <tr><td>TEST 00 BHZ</td><td>Streckeisen STS-2</td></tr>
        <tr><td>TEST 10 HH1</td><td>Trillium Compact</td></tr>
        <tr><td>TEST 10 HH2</td><td>Trillium Compact</td></tr>
        <tr><td>TEST 10 HHZ</td><td>Trillium Compact</td></tr>
        <tr><td>TEST -- XXX</td><td>xxx</td></tr> 
      </tbody>
    </table>
    """
    mock_response = MagicMock()
    mock_response.text = FAKE_HTML
    return mocker.patch("sqes.utils.sensor_updater.requests.get", return_value=mock_response)

@pytest.fixture
def mock_pandas_sql(mocker):
    """Mocks pandas' SQL read and write functions."""
    fake_station_list = pd.DataFrame({'code': ['TEST']})
    mocker.patch("sqes.utils.sensor_updater.pd.read_sql", return_value=fake_station_list)
    
    mock_to_sql = mocker.patch("sqes.utils.sensor_updater.pd.DataFrame.to_sql")
    return mock_to_sql

@pytest.fixture
def mock_pandas_concat(mocker):
    """Mocks pd.concat to intercept the final DataFrame."""
    # We need to return a *real* function so that sensor_df.empty works
    # This mock just calls the real pd.concat but spies on it.
    mock_concat = MagicMock(wraps=pd.concat)
    mocker.patch("sqes.utils.sensor_updater.pd.concat", mock_concat)
    return mock_concat

@pytest.fixture
def mock_sqlalchemy(mocker):
    """
    Mocks the SQLAlchemy engine, the connection, and the transaction.
    """
    mock_execute = MagicMock()
    mock_conn = MagicMock()
    mock_conn.execute = mock_execute
    mock_conn.__enter__.return_value = mock_conn
    mock_conn.__exit__.return_value = None
    mock_engine = MagicMock()
    mock_engine.begin.return_value = mock_conn
    mock_engine.dispose = MagicMock()
    
    mocker.patch("sqes.utils.sensor_updater.create_engine", return_value=mock_engine)
    
    return mock_engine, mock_execute

# --- Tests ---

def test_data_updater_full_run(
    mock_requests_get, 
    mock_pandas_sql,  # This is the to_sql mock
    mock_pandas_concat, # This is the new concat mock
    mock_sqlalchemy
):
    """
    Tests the full, successful run of the sensor updater.
    """
    mock_engine, mock_execute = mock_sqlalchemy
    
    db_creds = {
        'host': 'localhost', 'port': 5432, 'database': 'test',
        'user': 'test', 'password': 'pw'
    }
    
    # Act: Run the function
    sensor_updater.update_sensor_table(
        db_type='postgresql',
        db_creds=db_creds,
        update_url="http://fake-url.com/{station_code}"
    )

    # --- Assertions ---

    # 1. Was the correct URL scraped?
    mock_requests_get.assert_called_with(
        "http://fake-url.com/TEST", timeout=10
    )
    
    # 2. Was the TRUNCATE command executed?
    mock_execute.assert_called_once()
    called_sql_command = str(mock_execute.call_args[0][0])
    assert "TRUNCATE TABLE stations_sensor" in called_sql_command

    # 3. Was to_sql called with the correct *arguments*?
    mock_pandas_sql.assert_called_once()
    # call_args[0] is the tuple of positional args: ('stations_sensor',)
    assert mock_pandas_sql.call_args[0][0] == 'stations_sensor'
    # call_args[1] is the dict of keyword args
    assert mock_pandas_sql.call_args[1]['if_exists'] == 'append'
    
    # 4. Was the *data* built correctly?
    # We check the DataFrame that was passed to pd.concat
    # call_args[0] is the args tuple, [0] is the list of DataFrames
    df_list_passed_to_concat = mock_pandas_concat.call_args[0][0]
    # The first item is the empty df, the second is our parsed data
    final_df = df_list_passed_to_concat[1] 
    
    # Should have 7 rows (including the 'xxx' row, *before* filtering)
    assert len(final_df) == 7
    assert 'BHE' in final_df['channel'].values
    assert '10' in final_df['location'].values

    # 5. Was the engine disposed?
    mock_engine.dispose.assert_called_once()