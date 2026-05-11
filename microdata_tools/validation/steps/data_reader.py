# pyright: reportAttributeAccessIssue=false
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Tuple

import pyarrow
import pyarrow.dataset
from pyarrow import ArrowInvalid, compute, csv, parquet

from microdata_tools.validation.exceptions import ValidationError

logger = logging.getLogger()


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


def _microdata_data_type_to_disk_pyarrow(
    microdata_data_type: str,
) -> pyarrow.lib.DataType:
    if microdata_data_type == "STRING":
        return pyarrow.string()
    elif microdata_data_type == "LONG":
        return pyarrow.int64()
    elif microdata_data_type == "DOUBLE":
        return pyarrow.float64()
    elif microdata_data_type == "DATE":
        return pyarrow.int64()
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


def _csv_stream_to_parquet(
    identifier_data_type: str,
    measure_data_type: str,
    temporality_type: str,
    reader: pyarrow.csv.CSVStreamingReader,
    writer: pyarrow.parquet.ParquetWriter,
) -> None:
    while True:
        try:
            batch = reader.read_next_batch()
        except StopIteration:
            break
        table = pyarrow.Table.from_batches([batch])
        unit_id = _sanitize_unit_id(table, identifier_data_type)
        value = _sanitize_value(table, measure_data_type)
        epoch_start = _cast_to_epoch_date(table, "start")
        epoch_stop = _cast_to_epoch_date(table, "stop")
        columns = [unit_id, value, epoch_start, epoch_stop]
        column_names = [
            "unit_id",
            "value",
            "start_epoch_days",
            "stop_epoch_days",
        ]
        if temporality_type in ["STATUS", "ACCUMULATED"]:
            columns.append(
                _generate_start_year(table)
            )  # <-- OOM #1 her ved 30M rader og 2GB ram
            column_names.append("start_year")
        table = pyarrow.Table.from_arrays(columns, column_names)
        writer.write_table(table)


def _csv_to_parquet(
    input_csv_path: Path,
    output_parquet_path: str,
    identifier_data_type: str,
    measure_data_type: str,
    temporality_type: str,
) -> pyarrow.dataset.FileSystemDataset:
    """
    Read a csv into a pyarrow table. The read and convert options
    ensures microdata formatting of the input csv.
    """
    try:
        with csv.open_csv(
            input_csv_path,
            parse_options=csv.ParseOptions(delimiter=";"),
            read_options=_get_csv_read_options(),
            convert_options=_get_csv_convert_options(
                identifier_data_type, measure_data_type
            ),
        ) as reader:
            schema_list = [
                (
                    "unit_id",
                    _microdata_data_type_to_pyarrow(identifier_data_type),
                ),
                (
                    "value",
                    _microdata_data_type_to_disk_pyarrow(measure_data_type),
                ),
                ("start_epoch_days", pyarrow.int16()),
                ("stop_epoch_days", pyarrow.int16()),
            ]
            if temporality_type in ["STATUS", "ACCUMULATED"]:
                schema_list.append(("start_year", pyarrow.string()))
            schema = pyarrow.schema(schema_list)
            with parquet.ParquetWriter(output_parquet_path, schema) as writer:
                _csv_stream_to_parquet(
                    identifier_data_type,
                    measure_data_type,
                    temporality_type,
                    reader,
                    writer,
                )
        return pyarrow.dataset.dataset(output_parquet_path)
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


def read_and_sanitize_csv_write_parquet(
    input_data_path: Path,
    output_parquet_path: Path,
    identifier_data_type: str,
    measure_data_type: str,
    temporality_type: str,
) -> pyarrow.dataset.FileSystemDataset:
    """
    Streams a csv file to a parquet file. Sanitizes values and
    ensures the input csv data follows the requirements for the
    microdata data model.
    """
    return _csv_to_parquet(
        input_data_path,
        output_parquet_path,
        identifier_data_type,
        measure_data_type,
        temporality_type,
    )


def _min_max(
    filesystem_dataset: pyarrow.dataset.FileSystemDataset, column: str
) -> Tuple[int | None, int | None]:
    min_v = None
    max_v = None
    for batch in filesystem_dataset.to_batches():
        maybe_min, maybe_max = compute.min_max(batch[column]).as_py().values()
        if maybe_min is None:
            pass
        elif min_v is None:
            min_v = maybe_min
        elif maybe_min < min_v:
            min_v = maybe_min

        if maybe_max is None:
            pass
        elif max_v is None:
            max_v = maybe_max
        elif maybe_max > max_v:
            max_v = maybe_max
    return min_v, max_v


def _max(
    filesystem_dataset: pyarrow.dataset.FileSystemDataset, column: str
) -> Tuple[int | None, int | None]:
    return _min_max(filesystem_dataset, column)[1]


def _min(
    filesystem_dataset: pyarrow.dataset.FileSystemDataset, column: str
) -> Tuple[int | None, int | None]:
    return _min_max(filesystem_dataset, column)[0]


def get_temporal_data(
    dataset: pyarrow.dataset.FileSystemDataset, temporality_type: str
) -> Dict[str, int]:
    """
    Reads the temporal columns of the pyarrow.Table and
    returns a dictionary with information depending on the
    temporality_type of the data.
    """
    temporal_data = {}
    if temporality_type == "FIXED":
        stop_max = _max(dataset, "stop_epoch_days")
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
        start_min, start_max = _min_max(dataset, "start_epoch_days")
        stop_min, stop_max = _min_max(dataset, "stop_epoch_days")
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
                dataset.to_table(columns=["start_epoch_days"])[
                    "start_epoch_days"
                ]
            ).to_pylist()
        ]
    return temporal_data
