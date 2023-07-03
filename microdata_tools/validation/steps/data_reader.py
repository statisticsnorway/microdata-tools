# pylint: disable=no-member
import logging
from pathlib import Path
from typing import Dict
from datetime import datetime
from datetime import timedelta

import pyarrow
from pyarrow import csv
from pyarrow import compute, ArrowInvalid


logger = logging.getLogger()


def get_temporal_data(
    table: pyarrow.Table, temporality_type: str
) -> Dict[str, int]:
    temporal_data = {}
    if temporality_type == "FIXED":
        stop_max = compute.max(table["epoch_stop"]).as_py()
        temporal_data["start"] = "1900-01-01"
        temporal_data["latest"] = (
            datetime(1970, 1, 1) + timedelta(days=stop_max)
        ).strftime("%Y-%m-%d")
    else:
        start_min, start_max = (
            compute.min_max(table["epoch_start"]).as_py().values()
        )
        stop_min, stop_max = (
            compute.min_max(table["epoch_stop"]).as_py().values()
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
            for status_days in compute.unique(table["epoch_start"]).to_pylist()
        ]
    return temporal_data


def get_csv_read_options():
    return csv.ReadOptions(
        column_names=["identifier", "measure", "start", "stop", "attributes"]
    )


def get_csv_convert_options(measure_data_type: str):
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
        raise ValueError("TODO")
    return csv.ConvertOptions(
        column_types={
            "identifier": pyarrow.string(),
            "measure": pyarrow_data_type,
            "start": pyarrow.date32(),
            "stop": pyarrow.date32(),
            "attributes": pyarrow.string(),
        }
    )


def sanitize_data(
    input_data_path: Path, measure_data_type: str
) -> pyarrow.Table:
    try:
        table = csv.read_csv(
            input_data_path,
            parse_options=csv.ParseOptions(delimiter=";"),
            read_options=get_csv_read_options(),
            convert_options=get_csv_convert_options(measure_data_type),
        )
    except ArrowInvalid as e:
        raise ValueError(str(e)) from e

    identifier = compute.utf8_trim(table["identifier"], " ")
    measure = (
        table["measure"]
        if measure_data_type != "STRING"
        else compute.utf8_trim(table["measure"], " ")
    )
    epoch_start = compute.cast(table["start"], target_type=pyarrow.int32())
    epoch_stop = compute.cast(table["stop"], target_type=pyarrow.int32())
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
            "identifier",
            "measure",
            "start_year",
            "epoch_start",
            "epoch_stop",
        ],
    )


def metadata_update_temporal_coverage(
    metadata: dict, temporal_data: dict
) -> None:
    logger.debug(
        "Append temporal coverage (start, stop, status dates) to metadata"
    )
    data_revision = metadata["dataRevision"]
    temporality_type = metadata["temporalityType"]
    data_revision["temporalCoverageStart"] = temporal_data["start"]
    data_revision["temporalCoverageLatest"] = temporal_data["latest"]
    if temporality_type == "STATUS":
        temporal_status_dates_list = temporal_data["statusDates"]
        temporal_status_dates_list.sort()
        data_revision["temporalStatusDates"] = temporal_status_dates_list


def run_reader(
    dataset_name: str, input_directory: Path, measure_data_type: str
) -> pyarrow.Table:
    input_dataset_dir = input_directory / dataset_name
    input_data_path = input_dataset_dir / f"{dataset_name}.csv"

    logger.debug(f'Start reading dataset "{dataset_name}"')
    table = sanitize_data(input_data_path, measure_data_type)

    logger.debug(f'OK - reading dataset "{dataset_name}"')
    return table
