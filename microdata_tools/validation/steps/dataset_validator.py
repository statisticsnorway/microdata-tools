# pyright: reportAttributeAccessIssue=false
import os.path
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import List, Union

from pyarrow import Table, dataset
from pyarrow.dataset import FileSystemDataset

from microdata_tools.validation.exceptions import ValidationError


def _get_error_list(invalid_rows: Table, message: str) -> list[str]:
    invalid_identifiers = (
        invalid_rows.column("unit_id").slice(0, 50).to_pylist()
    )
    return [
        f"{message} for row with identifier: {identifier}"
        for identifier in invalid_identifiers
    ]


def _valid_value_column_check(
    data: FileSystemDataset,
    data_type: str,
    code_list: Union[List, None],
    sentinel_list: Union[List, None],
) -> None:
    """
    Any given cell in the value column is valid only if:
    * The cell contains a a valid non-null value
    * The cell does not contain an empty string if data_type is STRING
    * The value is present in the code_list if supplied
    """
    is_null_filter = dataset.field("value").is_null()
    is_empty_string_filter = dataset.field("value") == ""
    invalid_rows_filter = (
        (is_null_filter | is_empty_string_filter)
        if data_type == "STRING"
        else is_null_filter
    )
    invalid_rows = data.to_table(
        filter=invalid_rows_filter, columns=["unit_id"]
    )
    if len(invalid_rows) > 0:
        raise ValidationError(
            "#2 column",
            errors=_get_error_list(invalid_rows, "Invalid value in #2 column"),
        )

    if code_list:
        unique_codes = list(
            set(code_list_item["code"] for code_list_item in code_list)
        )
        if sentinel_list:
            unique_codes += list(
                set(
                    sentinel_list_item["code"]
                    for sentinel_list_item in sentinel_list
                )
            )
        invalid_code_filter = ~dataset.field("value").isin(unique_codes)
        invalid_rows = data.to_table(
            filter=invalid_code_filter, columns=["unit_id", "value"]
        )
        if len(invalid_rows) > 0:
            invalid_codes = (
                invalid_rows.column("value").slice(0, 50).to_pylist()
            )
            invalid_unit_ids = (
                invalid_rows.column("unit_id").slice(0, 50).to_pylist()
            )
            invalid_code_rows = list(zip(invalid_unit_ids, invalid_codes))

            raise ValidationError(
                "#2 column",
                errors=[
                    f"Error for identifier {unit_id}: {code} is not in "
                    f"code list"
                    for (unit_id, code) in invalid_code_rows
                ],
            )


def _valid_unit_id_check(data: FileSystemDataset) -> None:
    """
    Any given cell in the unit_id column is valid only if:
    * The cell contains a a valid non-null value
    * The cell does not contain an empty string
    """
    is_null_filter = dataset.field("unit_id").is_null()
    is_empty_string_filter = dataset.field("unit_id") == ""
    invalid_rows_filter = is_null_filter | is_empty_string_filter
    invalid_rows = data.to_table(
        filter=invalid_rows_filter, columns=["unit_id"]
    )
    if len(invalid_rows) > 0:
        raise ValidationError(
            "#1 column",
            errors=_get_error_list(
                invalid_rows, "Invalid identifier in #1 column"
            ),
        )


def _fixed_temporal_variables_check(data: FileSystemDataset) -> None:
    """
    Any given row in a table with temporalityType=FIXED is valid only if:
    * The start_epoch_days column contains null (empty)
    * The stop_epoch_days column contains a non-null value (int32)
    """
    start_is_valid_filter = dataset.field("start_epoch_days").is_valid()
    stop_is_null_filter = dataset.field("stop_epoch_days").is_null()
    invalid_rows = data.to_table(
        filter=start_is_valid_filter | stop_is_null_filter,
        columns=["unit_id"],
    )
    if len(invalid_rows) > 0:
        raise ValidationError(
            "#3 and #4 columns",
            errors=_get_error_list(
                invalid_rows, "Invalid #3 and/or #4 columns"
            ),
        )


def _status_temporal_variables_check(data: FileSystemDataset) -> None:
    """
    Any given row in a table with temporalityType=STATUS is valid only if:
    * The start_epoch_days column contains a non-null value (int32)
    * The stop_epoch_days column contains a non-null value (int32)
    * The start_epoch_days and stop_epoch_days columns contain the same value
      for any given row
    """
    invalid_rows = data.to_table(
        filter=(
            dataset.field("stop_epoch_days").is_null()
            | dataset.field("start_epoch_days").is_null()
        ),
        columns=["unit_id"],
    )
    if len(invalid_rows) > 0:
        raise ValidationError(
            "#3 and #4 columns",
            errors=_get_error_list(
                invalid_rows, "Invalid #3 and/or #4 columns"
            ),
        )
    invalid_rows = data.to_table(
        filter=dataset.field("start_epoch_days")
        != dataset.field("stop_epoch_days"),
        columns=["unit_id"],
    )
    if len(invalid_rows) > 0:
        raise ValidationError(
            "#3 and #4 columns",
            errors=_get_error_list(
                invalid_rows, "#3 column not equal to #4 column"
            ),
        )


def _event_temporal_variables_check(data: FileSystemDataset) -> None:
    """
    Any given row in a table with temporalityType=EVENT is valid only if:
    * The start_epoch_days column contains a non-null value (int32)
    * The stop_epoch_days is either a non-null value bigger than or equal to
      start_epoch_days (int32), or null (empty)
    """
    start_is_null_filter = dataset.field("start_epoch_days").is_null()
    start_bt_stop_filter = dataset.field("start_epoch_days") > dataset.field(
        "stop_epoch_days"
    )  # If stop_epoch_days is null this test will be ignored by pyarrow
    invalid_rows = data.to_table(
        filter=(start_is_null_filter | start_bt_stop_filter),
        columns=["unit_id"],
    )
    if len(invalid_rows) > 0:
        raise ValidationError(
            "#3 and #4 columns",
            errors=_get_error_list(
                invalid_rows, "Invalid #3 and/or #4 columns"
            ),
        )


def _accumulated_temporal_variables_check(data: FileSystemDataset) -> None:
    """
    Any given row in a table with temporalityType=ACCUMULATED is valid only if:
    * The start_epoch_days column contains a non-null value (int32)
    * The stop_epoch_days is non-null (int32) value bigger than start_epoch_days
    """
    start_is_null_filter = dataset.field("start_epoch_days").is_null()
    stop_is_null_filter = dataset.field("stop_epoch_days").is_null()
    start_be_stop_filter = dataset.field("start_epoch_days") >= dataset.field(
        "stop_epoch_days"
    )
    invalid_rows = data.to_table(
        filter=(
            start_is_null_filter | stop_is_null_filter | start_be_stop_filter
        ),
        columns=["unit_id"],
    )
    if len(invalid_rows) > 0:
        raise ValidationError(
            "#3 and #4 columns",
            errors=_get_error_list(
                invalid_rows, "Invalid #3 and/or #4 columns"
            ),
        )


def _only_unique_identifiers_check(conn: sqlite3.Connection) -> None:
    """
    A table with temporalityType=FIXED is only valid if all
    cells in the unit_id column are unique.
    """
    prev_unit_id = None
    for (unit_id,) in conn.execute(
        "SELECT unit_id FROM dataset ORDER BY unit_id ASC"
    ):
        if unit_id == prev_unit_id:
            raise ValidationError(
                "#1 column",
                errors=["Duplicate identifiers in #1 column"],
            )
        else:
            prev_unit_id = unit_id


def _status_uniquesness_check(conn: sqlite3.Connection) -> None:
    """
    A table with temporalityType=STATUS is valid only if all
    cells in the unit_id column are unique per status date.
    """
    prev = None, None
    for curr in conn.execute(
        "SELECT unit_id, start_epoch_days FROM dataset "
        + "ORDER BY unit_id ASC, start_epoch_days ASC"
    ):
        if curr == prev:
            raise ValidationError(
                "#1, #3 and #4 columns",
                errors=[
                    "Same unit_id (#1 Column) has duplicate dates "
                    "(#3 and #4 column)"
                ],
            )
        else:
            prev = curr


def _from_epoch_days_to_date(epoch_days: Union[int, None]) -> str:
    return (
        ""
        if epoch_days is None
        else datetime.fromtimestamp(epoch_days * 24 * 60 * 60).strftime(
            "%Y-%m-%d"
        )
    )


def _find_overlap(start_list: list, stop_list: list) -> Union[str, None]:
    """
    Looks for overlapping timespans where each timespan
    is defined by a start_date at an index from the start_list,
    and a stop_date at the same index from the stop_list.
    """
    for i in range(len(start_list) - 1):
        if stop_list[i] is None:
            return (
                f"timespan: ({_from_epoch_days_to_date(start_list[i])} - "
                ") overlaps with "
                f"timespan: "
                f"({_from_epoch_days_to_date(start_list[i + 1])} - "
                f"{_from_epoch_days_to_date(stop_list[i + 1])})"
            )
        if stop_list[i] >= start_list[i + 1]:
            return (
                f"timespan: ({_from_epoch_days_to_date(start_list[i])} - "
                f"{_from_epoch_days_to_date(stop_list[i])}) "
                f"overlaps with timespan: "
                f"({_from_epoch_days_to_date(start_list[i + 1])} - "
                f"{_from_epoch_days_to_date(stop_list[i + 1])})"
            )
    return None


def _check_overlap(
    error_list: list[str], unit_id: str, start_list: list, stop_list: list
) -> None:
    overlap_message = _find_overlap(start_list, stop_list)
    if overlap_message is not None:
        error_list.append(
            (
                "Invalid overlapping timespans for identifier"
                f' "{unit_id}":'
                f" {overlap_message}"
            )
        )
        if len(error_list) > 49:
            raise ValidationError(
                "#1, #3 and #4 columns",
                errors=error_list,
            )


def _no_overlapping_timespans_check(
    conn: sqlite3.Connection,
) -> None:
    """
    A table with temporalityType=(EVENT|ACCUMULATED) is valid
    only if all rows for a given identifier contains no overlapping
    timespans in the start_epoch_days and stop_epoch_days columns.
    """
    error_list = []
    cursor = conn.cursor()
    cursor.execute(
        "SELECT unit_id, start_epoch_days, stop_epoch_days "
        + "FROM dataset "
        + "ORDER BY unit_id, start_epoch_days"
    )
    curr_unit_id = None
    start_list = []
    stop_list = []
    while True:
        res = cursor.fetchone()
        if res is None:
            if curr_unit_id is not None:
                _check_overlap(error_list, curr_unit_id, start_list, stop_list)
            break
        else:
            unit_id, start_epoch_days, stop_epoch_days = res
            if curr_unit_id is None:
                curr_unit_id = unit_id
                start_list.append(start_epoch_days)
                stop_list.append(stop_epoch_days)
            elif curr_unit_id == unit_id:
                start_list.append(start_epoch_days)
                stop_list.append(stop_epoch_days)
            elif curr_unit_id != unit_id:
                _check_overlap(error_list, curr_unit_id, start_list, stop_list)
                curr_unit_id = unit_id
                start_list = [start_epoch_days]
                stop_list = [stop_epoch_days]
            else:
                raise RuntimeError("Unhandled state!")
    if error_list:
        raise ValidationError(
            "#1, #3 and #4 columns",
            errors=error_list,
        )


def validate_dataset(
    data: FileSystemDataset,
    sqlite_path: Path,
    measure_data_type: str,
    code_list: Union[List, None],
    sentinel_list: Union[List, None],
    temporality_type: str,
) -> None:
    assert os.path.exists(sqlite_path)
    with sqlite3.connect(sqlite_path) as conn:
        _valid_unit_id_check(data)
        _valid_value_column_check(
            data, measure_data_type, code_list, sentinel_list
        )
        if temporality_type == "FIXED":
            _fixed_temporal_variables_check(data)
            _only_unique_identifiers_check(conn)
        elif temporality_type == "STATUS":
            _status_temporal_variables_check(data)
            _status_uniquesness_check(conn)
        elif temporality_type == "ACCUMULATED":
            _accumulated_temporal_variables_check(data)
            _no_overlapping_timespans_check(conn)
        elif temporality_type == "EVENT":
            _event_temporal_variables_check(data)
            _no_overlapping_timespans_check(conn)
        else:
            raise RuntimeError(f"Unknown temporality type '{temporality_type}'")
