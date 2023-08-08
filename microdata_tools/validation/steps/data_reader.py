# pylint: disable=no-member
import logging
from pathlib import Path
from typing import Dict
from datetime import datetime
from datetime import timedelta

import pyarrow
from pyarrow import csv
from pyarrow import compute, ArrowInvalid

from microdata_tools.validation.exceptions import ValidationError


logger = logging.getLogger()


def get_temporal_data(
    table: pyarrow.Table, temporality_type: str
) -> Dict[str, int]:
    temporal_data = {}
    if temporality_type == "FIXED":
        stop_max = compute.max(table["stop_epoch_days"]).as_py()
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
    return temporal_data


def _get_csv_read_options():
    return csv.ReadOptions(
        column_names=["unit_id", "value", "start", "stop", "attributes"]
    )


def _get_csv_convert_options(measure_data_type: str):
    pyarrow_data_type = None
    if measure_data_type == "STRING":
        pyarrow_data_type = pyarrow.string()
    elif measure_data_type == "LONG":
        pyarrow_data_type = pyarrow.int64()
    elif measure_data_type == "DOUBLE":
        pyarrow_data_type = pyarrow.float64()
    elif measure_data_type == "DATE":
        pyarrow_data_type = pyarrow.date32()
    else:
        raise ValidationError(
            "Unsupported measure data type",
            errors=[f"Unsupported measure data type: {measure_data_type}"],
        )
    return csv.ConvertOptions(
        column_types={
            "unit_id": pyarrow.string(),
            "value": pyarrow_data_type,
            "start": pyarrow.date32(),
            "stop": pyarrow.date32(),
            "attributes": pyarrow.string(),
        }
    )


def _sanitize_data(
    input_data_path: Path, measure_data_type: str
) -> pyarrow.Table:
    try:
        table = csv.read_csv(
            input_data_path,
            parse_options=csv.ParseOptions(delimiter=";"),
            read_options=_get_csv_read_options(),
            convert_options=_get_csv_convert_options(measure_data_type),
        )
    except ArrowInvalid as e:
        raise ValidationError(
            "Error when reading dataset", errors=[str(e)]
        ) from e

    identifier = compute.utf8_trim(table["unit_id"], " ")
    measure = (
        table["value"]
        if measure_data_type != "STRING"
        else compute.utf8_trim(table["value"], " ")
    )
    if measure_data_type == "DATE":
        measure = measure.cast(pyarrow.int32()).cast(pyarrow.int64())
    epoch_start = table["start"].cast(pyarrow.int32()).cast(pyarrow.int16())
    epoch_stop = table["stop"].cast(pyarrow.int32()).cast(pyarrow.int16())
    start_year = compute.utf8_slice_codeunits(
        table["start"].cast(pyarrow.string()), start=0, stop=4
    )

    # generate enriched table
    return pyarrow.Table.from_arrays(
        [
            identifier,
            measure,
            start_year,
            epoch_start,
            epoch_stop,
        ],
        names=[
            "unit_id",
            "value",
            "start_year",
            "start_epoch_days",
            "stop_epoch_days",
        ],
    )


def run_reader(input_data_path: Path, measure_data_type: str) -> pyarrow.Table:
    table = _sanitize_data(input_data_path, measure_data_type)
    return table
