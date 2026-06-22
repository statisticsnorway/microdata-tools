import os
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Union

import pytest

from microdata_tools.validation.exceptions import ValidationError
from microdata_tools.validation.steps import data_reader, dataset_validator
from tests import test_data


def teardown_module():
    if os.path.exists("tmp"):
        shutil.rmtree("tmp/")


def _dict_to_dataset_and_sqlite(
    table_dict: Dict,
    identifier_data_type: str,
    measure_data_type: str,
    temporality_data_type: str,
):
    columns = ["unit_id", "value", "start_epoch_days", "stop_epoch_days"]
    columns = [col for col in columns if col in table_dict]
    lengths = [len(table_dict[col]) for col in columns]
    length = max(lengths)
    assert length == min(lengths)

    os.makedirs("tmp", exist_ok=True)
    csv_path = "tmp/tmp.csv"
    with open(csv_path, "w") as f:
        for i in range(length):
            first = True
            for col in columns:
                if first:
                    first = False
                else:
                    f.write(";")
                v = table_dict[col][i]
                if v is not None:
                    if col == "value" and measure_data_type == "DATE":
                        dt = datetime(1970, 1, 1) + timedelta(days=v)
                        dt_str = dt.strftime("%Y-%m-%d")
                        f.write(dt_str)
                    elif col == "stop_epoch_days" or col == "start_epoch_days":
                        dt = datetime(1970, 1, 1) + timedelta(days=v)
                        dt_str = dt.strftime("%Y-%m-%d")
                        f.write(dt_str)
                    else:
                        f.write(str(v))
            for _ in range(5 - len(columns)):
                f.write(";")
            f.write("\n")
    filesystem_dataset = (
        data_reader.read_and_sanitize_csv_write_sqlite_and_parquet(
            Path(csv_path),
            Path("tmp/tmp.parquet"),
            Path("tmp/tmp.sqlite3.db"),
            identifier_data_type,
            measure_data_type,
            temporality_data_type,
        )
    )
    return filesystem_dataset


def _csv_string_to_dataset_and_sqlite(
    csv: str,
    identifier_data_type: str,
    measure_data_type: str,
    temporality_data_type: str,
):
    csv = csv.strip()
    os.makedirs("tmp", exist_ok=True)
    csv_path = "tmp/tmp.csv"
    with open(csv_path, "w") as f:
        for lin in csv.splitlines():
            lin = lin.strip()
            f.write(lin)
            f.write("\n")
    return data_reader.read_and_sanitize_csv_write_sqlite_and_parquet(
        Path(csv_path),
        Path("tmp/tmp.parquet"),
        Path("tmp/tmp.sqlite3.db"),
        identifier_data_type,
        measure_data_type,
        temporality_data_type,
    )


def _validate_dataset(
    table_dict: Dict,
    identifier_data_type: str,
    measure_data_type: str,
    code_list: Union[List, None],
    sentinel_list: Union[List, None],
    temporality_type: str,
):
    dataset = _dict_to_dataset_and_sqlite(
        table_dict, identifier_data_type, measure_data_type, temporality_type
    )
    dataset_validator.validate_dataset(
        dataset,
        Path("tmp/tmp.sqlite3.db"),
        measure_data_type,
        code_list,
        sentinel_list,
        temporality_type,
    )


def test_measure_code_list_validation():
    code_list = test_data.FIXED_STRING_CODELIST
    sentinel_list = test_data.FIXED_STRING_CODELIST_SENTINEL
    _validate_dataset(
        test_data.FIXED_STRING_CODELIST_DS(),
        "STRING",
        "STRING",
        code_list,
        sentinel_list,
        "FIXED",
    )
    with pytest.raises(ValidationError) as e:
        _validate_dataset(
            test_data.FIXED_STRING_CODELIST_INVALID_DS(),
            "STRING",
            "STRING",
            code_list,
            sentinel_list,
            "FIXED",
        )
    assert e.value.errors == ["Error for identifier 4: 3 is not in code list"]


def test_measure_data_type_string_validation():
    _validate_dataset(
        test_data.FIXED_STRING_DS(),
        "STRING",
        "STRING",
        None,
        None,
        "FIXED",
    )
    with pytest.raises(ValidationError) as e:
        _validate_dataset(
            test_data.FIXED_STRING_INVALID_DS(),
            "STRING",
            "STRING",
            None,
            None,
            "FIXED",
        )
    assert e.value.errors == [
        "Invalid value in #2 column for row with identifier: 2",
        "Invalid value in #2 column for row with identifier: 3",
    ]


def test_measure_data_type_long_validation():
    _validate_dataset(
        test_data.FIXED_LONG_DS(),
        "STRING",
        "LONG",
        None,
        None,
        "FIXED",
    )
    with pytest.raises(ValidationError) as e:
        _validate_dataset(
            test_data.FIXED_LONG_INVALID_DS(),
            "STRING",
            "LONG",
            None,
            None,
            "FIXED",
        )
    assert e.value.errors == [
        "Invalid value in #2 column for row with identifier: 4"
    ]


def test_measure_data_type_double_validation():
    _validate_dataset(
        test_data.FIXED_DOUBLE_DS(),
        "STRING",
        "DOUBLE",
        None,
        None,
        "FIXED",
    )
    with pytest.raises(ValidationError) as e:
        _validate_dataset(
            test_data.FIXED_DOUBLE_INVALID_DS(),
            "STRING",
            "DOUBLE",
            None,
            None,
            "FIXED",
        )
    assert e.value.errors == [
        "Invalid value in #2 column for row with identifier: 4"
    ]


def test_measure_data_type_date_validation():
    _validate_dataset(
        test_data.FIXED_DATE_DS(),
        "STRING",
        "DATE",
        None,
        None,
        "FIXED",
    )
    with pytest.raises(ValidationError) as e:
        _validate_dataset(
            test_data.FIXED_DATE_INVALID_DS(),
            "STRING",
            "DATE",
            None,
            None,
            "FIXED",
        )
    assert e.value.errors == [
        "Invalid value in #2 column for row with identifier: 4"
    ]


def test_temporality_fixed():
    _validate_dataset(
        test_data.FIXED_VALID_DS(),
        "STRING",
        "STRING",
        None,
        None,
        "FIXED",
    )
    with pytest.raises(ValidationError) as e:
        _validate_dataset(
            test_data.FIXED_INVALID_START_DS(),
            "STRING",
            "STRING",
            None,
            None,
            "FIXED",
        )
    assert e.value.errors == [
        f"Invalid #3 and/or #4 columns for row with identifier: {id}"
        for id in range(1, 5)
    ]
    with pytest.raises(ValidationError) as e:
        _validate_dataset(
            test_data.FIXED_INVALID_DUPLICATES_DS(),
            "STRING",
            "STRING",
            None,
            None,
            "FIXED",
        )
    assert e.value.errors == ["Duplicate identifiers in #1 column"]


def test_temporality_status():
    _validate_dataset(
        test_data.STATUS_VALID_DS(),
        "STRING",
        "STRING",
        None,
        None,
        "STATUS",
    )
    with pytest.raises(ValidationError) as e:
        _validate_dataset(
            test_data.STATUS_INVALID_START_STOP_DS(),
            "STRING",
            "STRING",
            None,
            None,
            "STATUS",
        )
    assert e.value.errors == [
        f"#3 column not equal to #4 column for row with identifier: {id}"
        for id in range(1, 5)
    ]
    with pytest.raises(ValidationError) as e:
        _validate_dataset(
            test_data.STATUS_INVALID_DUPLICATES_DS(),
            "STRING",
            "STRING",
            None,
            None,
            "STATUS",
        )
    assert e.value.errors == [
        "Same unit_id (#1 Column) has duplicate dates (#3 and #4 column)"
    ]


def test_temporality_event():
    _validate_dataset(
        test_data.EVENT_VALID_DS(),
        "STRING",
        "STRING",
        None,
        None,
        "EVENT",
    )
    with pytest.raises(ValidationError) as e:
        _validate_dataset(
            test_data.EVENT_INVALID_START_DS(),
            "STRING",
            "STRING",
            None,
            None,
            "EVENT",
        )
    assert e.value.errors == [
        "Invalid #3 and/or #4 columns for row with identifier: 1"
    ]
    with pytest.raises(ValidationError) as e:
        _validate_dataset(
            test_data.EVENT_INVALID_OVERLAP_DS(),
            "STRING",
            "STRING",
            None,
            None,
            "EVENT",
        )
    assert e.value.errors == [
        (
            'Invalid overlapping timespans for identifier "1": '
            "timespan: (2020-12-30 - 2021-02-12) overlaps with "
            "timespan: (2021-02-12 - 2021-02-22)"
        )
    ]

    with pytest.raises(ValidationError) as e:
        _validate_dataset(
            test_data.EVENT_INVALID_TIMESPANS_DS(),
            "STRING",
            "STRING",
            None,
            None,
            "EVENT",
        )
    assert e.value.errors == [
        (
            'Invalid overlapping timespans for identifier "1": '
            "timespan: (2020-12-30 - 2021-02-12) overlaps with "
            "timespan: (2020-12-31 - 2021-02-22)"
        )
    ]
    with pytest.raises(ValidationError) as e:
        _validate_dataset(
            test_data.EVENT_TOO_MANY_ERRORS_DS(),
            "STRING",
            "STRING",
            None,
            None,
            "EVENT",
        )
    assert len(e.value.errors) == 50


def test_temporality_accumulated():
    _validate_dataset(
        test_data.ACCUMULATED_VALID_DS(),
        "STRING",
        "STRING",
        None,
        None,
        "ACCUMULATED",
    )
    with pytest.raises(ValidationError) as e:
        _validate_dataset(
            test_data.ACCUMULATED_INVALID_START_STOP_DS(),
            "STRING",
            "STRING",
            None,
            None,
            "ACCUMULATED",
        )
    assert e.value.errors == [
        "Invalid #3 and/or #4 columns for row with identifier: 1",
        "Invalid #3 and/or #4 columns for row with identifier: 1",
        "Invalid #3 and/or #4 columns for row with identifier: 2",
    ]
    with pytest.raises(ValidationError) as e:
        _validate_dataset(
            test_data.ACCUMULATED_INVALID_TIMESPANS_DS(),
            "STRING",
            "STRING",
            None,
            None,
            "ACCUMULATED",
        )
    assert e.value.errors == [
        (
            'Invalid overlapping timespans for identifier "1": '
            "timespan: (2020-12-30 - 2021-02-12) overlaps with "
            "timespan: (2020-12-31 - 2021-02-14)"
        ),
    ]


def _validate_csv(csv, temporality_type):
    identifier_data_type = "STRING"
    measure_data_type = "STRING"
    ds = _csv_string_to_dataset_and_sqlite(
        csv, identifier_data_type, measure_data_type, temporality_type
    )
    dataset_validator.validate_dataset(
        ds,
        Path("tmp/tmp.sqlite3.db"),
        measure_data_type,
        None,
        None,
        temporality_type,
    )


def test_max_50_errors():
    # codelist error
    with pytest.raises(ValidationError) as e:
        _validate_dataset(
            test_data.TOO_MANY_ERRORS_DS(),
            "STRING",
            "STRING",
            test_data.TOO_MANY_ERRORS_CODELIST,
            None,
            "ACCUMULATED",
        )
    assert len(e.value.errors) == 50

    # temporal error
    with pytest.raises(ValidationError) as e:
        _validate_dataset(
            test_data.TOO_MANY_ERRORS_DS(),
            "STRING",
            "STRING",
            None,
            None,
            "FIXED",
        )
        assert len(e.value.errors) == 50
