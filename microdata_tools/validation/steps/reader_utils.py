# pyright: reportAttributeAccessIssue=false
import logging
import os
import subprocess
import sys
from pathlib import Path

import pyarrow
from pyarrow import compute, csv

from microdata_tools.validation.exceptions import ValidationError

logger = logging.getLogger()


def get_csv_read_options() -> csv.ReadOptions:
    return csv.ReadOptions(
        # block_size=50*1024*1024,
        column_names=["unit_id", "value", "start", "stop", "attributes"]
    )


def microdata_data_type_to_pyarrow(
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


def get_csv_convert_options(
    identifier_data_type: str, measure_data_type: str
) -> csv.ConvertOptions:
    identifier_pyarrow_type = microdata_data_type_to_pyarrow(
        identifier_data_type
    )
    measure_pyarrow_type = microdata_data_type_to_pyarrow(measure_data_type)
    return csv.ConvertOptions(
        column_types={
            "unit_id": identifier_pyarrow_type,
            "value": measure_pyarrow_type,
            "start": pyarrow.date32(),
            "stop": pyarrow.date32(),
            "attributes": pyarrow.string(),
        }
    )


def _sanitize_value2(
    batch: pyarrow.RecordBatch, measure_data_type: str
) -> pyarrow.Array:
    if measure_data_type == "STRING":
        return compute.utf8_trim(batch["value"], " ")
    elif measure_data_type == "DATE":
        return batch["value"].cast(pyarrow.int32()).cast(pyarrow.int64())
    else:
        return batch["value"]


def _cast_to_epoch_date2(
    batch: pyarrow.RecordBatch, column_name: str
) -> pyarrow.Array:
    """
    Cast column from pyarrow date (YYYY-MM-DD) to unix epoch days.
    """
    return batch[column_name].cast(pyarrow.int32()).cast(pyarrow.int16())


def _generate_start_year2(batch: pyarrow.RecordBatch) -> pyarrow.Array:
    """
    Generates a start year array by substringing the "start" column
    with pyarrow dates (YYYY-MM-DD) to a string pyarrow.Array (YYYY)
    """
    return compute.utf8_slice_codeunits(
        batch["start"].cast(pyarrow.string()), start=0, stop=4
    )


def get_row_count(csv_path: Path) -> int:
    if not os.path.exists(csv_path):
        return -1

    rowcount_path = Path(str(csv_path) + ".rowcount")
    if os.path.exists(rowcount_path) and os.path.getmtime(
        rowcount_path
    ) >= os.path.getmtime(csv_path):
        with open(rowcount_path) as f:
            return int(f.read().strip())

    args = ["wc", "-l", csv_path]
    res = subprocess.run(args, capture_output=True)
    if res.returncode != 0:
        logger.error(f"Command {args}")
        logger.error(f"Failed with exit code {res.returncode}")
        sys.exit(1)
    else:
        sout = ""
        for lin in res.stdout.splitlines():
            try:
                sout += lin.decode("utf-8")
            except UnicodeDecodeError:
                sout += str(lin)
            sout += "\n"
        sout = sout.strip().split(" ")[0]
        with open(rowcount_path, "w", encoding="utf-8") as w:
            w.write(sout)
        return int(sout)
