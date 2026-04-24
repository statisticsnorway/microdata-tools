# pyright: reportAttributeAccessIssue=false
import logging
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict

import pyarrow
from pyarrow import ArrowInvalid, compute, csv

from microdata_tools.validation.exceptions import ValidationError

logger = logging.getLogger()


def current_milli_time() -> int:
    return time.time_ns() // 1_000_000


def _microdata_data_type_to_pyarrow(
    microdata_data_type: str,
) -> pyarrow.lib.DataType:
    if microdata_data_type == "STRING":
        return pyarrow.string()
    elif microdata_data_type == "LONG":
        return pyarrow.int64()
    elif microdata_data_type == "DOUBLE":
        return pyarrow.float64()
    elif microdata_data_type == "DATE":
        return pyarrow.date32()
    else:
        raise ValidationError(
            "Unsupported measure data type",
            errors=[f"Unsupported measure data type: {microdata_data_type}"],
        )


def _get_csv_read_options() -> csv.ReadOptions:
    return csv.ReadOptions(
        column_names=["unit_id", "value", "start", "stop", "attributes"]
    )


def _get_csv_convert_options(
    identifier_data_type: str, measure_data_type: str
) -> csv.ConvertOptions:
    identifier_pyarrow_type = _microdata_data_type_to_pyarrow(
        identifier_data_type
    )
    measure_pyarrow_type = _microdata_data_type_to_pyarrow(measure_data_type)
    return csv.ConvertOptions(
        column_types={
            "unit_id": identifier_pyarrow_type,
            "value": measure_pyarrow_type,
            "start": pyarrow.date32(),
            "stop": pyarrow.date32(),
            "attributes": pyarrow.string(),
        }
    )


def _csv_to_table(
    input_csv_path: Path, identifier_data_type: str, measure_data_type: str
) -> pyarrow.Table:
    """
    Read a csv into a pyarrow table. The read and convert options
    ensures microdata formatting of the input csv.
    """
    try:
        return csv.read_csv(
            input_csv_path,
            parse_options=csv.ParseOptions(delimiter=";"),
            read_options=_get_csv_read_options(),
            convert_options=_get_csv_convert_options(
                identifier_data_type, measure_data_type
            ),
        )
    except ArrowInvalid as e:
        raise ValidationError(
            "Error when reading dataset", errors=[str(e)]
        ) from e


def _sanitize_unit_id(
    table: pyarrow.Table, identifier_data_type: str
) -> pyarrow.Array:
    """
    Trim leading and trailing whitespace from the unit_id column
    """
    if identifier_data_type == "STRING":
        return compute.utf8_trim(table["unit_id"], " ")
    else:
        return table["unit_id"]


def _sanitize_value(
    table: pyarrow.Table, measure_data_type: str
) -> pyarrow.Array:
    """
    Sanitize the value column depending on the measure_data_type
    """
    if measure_data_type == "STRING":
        return compute.utf8_trim(table["value"], " ")
    elif measure_data_type == "DATE":
        return table["value"].cast(pyarrow.int32()).cast(pyarrow.int64())
    else:
        return table["value"]


def _cast_to_epoch_date(
    table: pyarrow.Table, column_name: str
) -> pyarrow.Array:
    """
    Cast column from pyarrow date (YYYY-MM-DD) to unix epoch days.
    """
    return table[column_name].cast(pyarrow.int32()).cast(pyarrow.int16())


def _generate_start_year(table: pyarrow.Table) -> pyarrow.Array:
    """
    Generates a start year array by substringing the "start" column
    with pyarrow dates (YYYY-MM-DD) to a string pyarrow.Array (YYYY)
    """
    return compute.utf8_slice_codeunits(
        table["start"].cast(pyarrow.string()), start=0, stop=4
    )


def read_and_sanitize_csv(
    input_data_path: Path,
    identifier_data_type: str,
    measure_data_type: str,
    temporality_type: str,
) -> pyarrow.Table:
    """
    Reads a csv file to a pyarrow table. Sanitizes values and
    ensures the input csv data follows the requirements for the
    microdata data model.
    """
    start_time = current_milli_time()
    table = _csv_to_table(
        input_data_path, identifier_data_type, measure_data_type
    )
    logger.debug(f"read_and_sanitize_csv row count: {len(table):_}")
    file_size = input_data_path.stat().st_size
    logger.debug("tick ...")
    unit_id = _sanitize_unit_id(table, identifier_data_type)
    logger.debug("tick ...")
    value = _sanitize_value(table, measure_data_type)
    logger.debug("tick ...")
    epoch_start = _cast_to_epoch_date(table, "start")
    logger.debug("tick ...")
    epoch_stop = _cast_to_epoch_date(table, "stop")
    logger.debug("tick ...")
    columns = [unit_id, value, epoch_start, epoch_stop]
    column_names = ["unit_id", "value", "start_epoch_days", "stop_epoch_days"]
    if temporality_type in ["STATUS", "ACCUMULATED"]:
        logger.debug("tick ...")
        columns.append(_generate_start_year(table))  # <== OOM her
        logger.debug("tick ...")
        column_names.append("start_year")
        logger.debug("tick ...")
    # print(f'number of rows: {len(unit_id):_}')
    logger.debug("tick ...")
    tbl = pyarrow.Table.from_arrays(columns, column_names)
    logger.debug("tick ...")
    spent_ms = current_milli_time() - start_time
    mb_per_s = (file_size / 1024 / 1024) / (spent_ms / 1000)
    logger.debug(f"read_and_sanitize_csv spent: {spent_ms:_} ms")
    logger.debug(f"read_and_sanitize_csv speed: {mb_per_s:.1f} MB/s")
    return tbl


def get_temporal_data(
    table: pyarrow.Table, temporality_type: str
) -> Dict[str, int]:
    """
    Reads the temporal columns of the pyarrow.Table and
    returns a dictionary with information depending on the
    temporality_type of the data.
    """
    start_time = current_milli_time()
    temporal_data = {}
    if temporality_type == "FIXED":
        stop_max = compute.max(table["stop_epoch_days"]).as_py()
        if stop_max is None:
            error_string = (
                "Could not read data in fourth column (Stop date)."
                " Is this column empty?"
            )
            raise ValidationError(error_string, errors=[error_string])
        temporal_data["start"] = "1900-01-01"
        temporal_data["latest"] = (
            datetime(1970, 1, 1) + timedelta(days=stop_max)
        ).strftime("%Y-%m-%d")
    else:
        start_min, start_max = (
            compute.min_max(table["start_epoch_days"]).as_py().values()
        )
        stop_min, stop_max = (
            compute.min_max(table["stop_epoch_days"]).as_py().values()
        )
        if start_min is None or start_max is None:
            error_string = (
                "Could not read data in third column (Start date)."
                " Is this column empty?"
            )
            raise ValidationError(error_string, errors=[error_string])
        if stop_min is None or stop_max is None:
            error_string = (
                "Could not read data in fourth column (Stop date)."
                " Is this column empty?"
            )
            raise ValidationError(error_string, errors=[error_string])
        min_date = min(
            [date for date in [start_min, stop_min] if date is not None]
        )
        max_date = max(
            [date for date in [start_max, stop_max] if date is not None]
        )
        temporal_data["start"] = (
            datetime(1970, 1, 1) + timedelta(days=min_date)
        ).strftime("%Y-%m-%d")
        temporal_data["latest"] = (
            datetime(1970, 1, 1) + timedelta(days=max_date)
        ).strftime("%Y-%m-%d")

    if temporality_type == "STATUS":
        temporal_data["statusDates"] = [
            (datetime(1970, 1, 1) + timedelta(days=status_days)).strftime(
                "%Y-%m-%d"
            )
            for status_days in compute.unique(
                table["start_epoch_days"]
            ).to_pylist()
        ]
    spent_ms = current_milli_time() - start_time
    logger.debug(f"get_temporal_data spent: {spent_ms:_} ms")
    return temporal_data
