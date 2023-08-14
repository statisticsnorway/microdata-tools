import pyarrow
from pathlib import Path

import pytest

from microdata_tools.validation.steps import data_reader
from microdata_tools.validation.exceptions import ValidationError


INPUT_DIR = Path("tests/resources/validation/steps/data_reader")

EXPECTED_COLUMNS = {
    "unit_id": ["000001", "000002", "000002"],
    "start_epoch_days": [None, 18262, 18262],
    "stop_epoch_days": [18262, 18262, 18262],
}


def test_get_temporal_data():
    table_schema = pyarrow.schema(
        [
            pyarrow.field("start_epoch_days", pyarrow.int16()),
            pyarrow.field("stop_epoch_days", pyarrow.int16()),
        ]
    )
    fixed_dict = {
        "start_epoch_days": [None, None, None, None, None],
        "stop_epoch_days": [1, 2, 3, 4, 5],
    }
    event_dict = {
        "start_epoch_days": [1, 2, 3, 1, 5],
        "stop_epoch_days": [2, 3, 4, 2, None],
    }
    accumulated_dict = {
        "start_epoch_days": [1, 2, 3, 1, 1],
        "stop_epoch_days": [2, 3, 4, 2, 5],
    }
    status_dict = {
        "start_epoch_days": [1, 2, 3, 4, 5],
        "stop_epoch_days": [1, 2, 3, 4, 5],
    }
    fixed_table = pyarrow.Table.from_pydict(fixed_dict, schema=table_schema)
    assert data_reader.get_temporal_data(fixed_table, "FIXED") == {
        "start": "1900-01-01",
        "latest": "1970-01-06",
    }
    event_table = pyarrow.Table.from_pydict(event_dict, schema=table_schema)
    assert data_reader.get_temporal_data(event_table, "EVENT") == {
        "start": "1970-01-02",
        "latest": "1970-01-06",
    }
    accumulated_table = pyarrow.Table.from_pydict(
        accumulated_dict, schema=table_schema
    )
    assert data_reader.get_temporal_data(accumulated_table, "ACCUMULATED") == {
        "start": "1970-01-02",
        "latest": "1970-01-06",
    }
    status_table = pyarrow.Table.from_pydict(status_dict, schema=table_schema)
    assert data_reader.get_temporal_data(status_table, "STATUS") == {
        "start": "1970-01-02",
        "latest": "1970-01-06",
        "statusDates": [
            "1970-01-02",
            "1970-01-03",
            "1970-01-04",
            "1970-01-05",
            "1970-01-06",
        ],
    }


def test_sanitize_long():
    long_data_path = INPUT_DIR / "LONG.csv"
    assert data_reader._sanitize_data(
        long_data_path, "LONG", "FIXED"
    ).to_pydict() == {
        **EXPECTED_COLUMNS,
        "value": [12345, 12345, 12345],
    }
    with pytest.raises(ValidationError) as e:
        invalid_data_path = INPUT_DIR / "LONG_INVALID_VALUE.csv"
        assert data_reader._sanitize_data(invalid_data_path, "LONG", "FIXED")
    assert e.value.errors == [
        "In CSV column #1: CSV conversion error to int64: invalid value 'abc123'"
    ]


def test_sanitize_double():
    double_data_path = INPUT_DIR / "DOUBLE.csv"
    assert data_reader._sanitize_data(
        double_data_path, "DOUBLE", "FIXED"
    ).to_pydict() == {
        **EXPECTED_COLUMNS,
        "value": [12345.12345, 12345.12345, 12345.12345],
    }
    with pytest.raises(ValidationError) as e:
        invalid_data_path = INPUT_DIR / "DOUBLE_INVALID_FORMAT.csv"
        assert data_reader._sanitize_data(invalid_data_path, "DOUBLE", "FIXED")
    assert e.value.errors == [
        "In CSV column #1: CSV conversion error to double: invalid value "
        "'12345,12345'"
    ]


def test_sanitize_date():
    date_data_path = INPUT_DIR / "DATE.csv"
    assert data_reader._sanitize_data(
        date_data_path, "DATE", "FIXED"
    ).to_pydict() == {
        **EXPECTED_COLUMNS,
        "value": [18262, 18262, 18262],
    }
    with pytest.raises(ValidationError) as e:
        invalid_data_path = INPUT_DIR / "DATE_INVALID_VALUE.csv"
        assert data_reader._sanitize_data(invalid_data_path, "DATE", "FIXED")
    assert e.value.errors == [
        "In CSV column #1: CSV conversion error to date32[day]: invalid value "
        "'2020-13-01'"
    ]


def test_sanitize_string():
    string_data_path = INPUT_DIR / "STRING.csv"
    assert data_reader._sanitize_data(
        string_data_path, "STRING", "FIXED"
    ).to_pydict() == {
        **EXPECTED_COLUMNS,
        "value": ["abc123", "abc123", "abc123"],
    }


def test_sanitize_data_invalid_start_stop():
    with pytest.raises(ValidationError) as e:
        invalid_data_path = INPUT_DIR / "STRING_INVALID_START_STOP.csv"
        assert data_reader._sanitize_data(invalid_data_path, "STRING", "FIXED")
    assert e.value.errors == [
        "In CSV column #3: CSV conversion error to date32[day]: invalid value "
        "'2020-13-01'"
    ]


def test_sanitize_data_wrong_delimiter():
    with pytest.raises(ValidationError) as e:
        invalid_data_path = INPUT_DIR / "STRING_INVALID_DELIMITER.csv"
        assert data_reader._sanitize_data(invalid_data_path, "STRING", "FIXED")
    assert e.value.errors == [
        "CSV parse error: Expected 5 columns, got 1: 000001,abc123,2020-01-01,"
    ]


def test_sanitize_data_empty_file():
    with pytest.raises(ValidationError) as e:
        invalid_data_path = INPUT_DIR / "STRING_EMPTY_FILE.csv"
        assert data_reader._sanitize_data(invalid_data_path, "STRING", "FIXED")
    assert e.value.errors == ["Empty CSV file"]
