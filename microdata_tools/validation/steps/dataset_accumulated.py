# pyright: reportAttributeAccessIssue=false
import logging
import os.path
import sqlite3
import time
from pathlib import Path
from typing import Dict, List, Union

import psutil
import pyarrow
from pyarrow import compute, csv, dataset, parquet

import microdata_tools.validation.steps.reader_utils as ru
from microdata_tools.validation.exceptions import ValidationError

logger = logging.getLogger()


def current_milli_time() -> int:
    return time.time_ns() // 1_000_000


def log_time() -> int:
    return time.time_ns() // 1_000_000 // 1000


def get_temporal_data2(
    table: pyarrow.Table, existing_data: Dict[str, int]
) -> Dict[str, int]:
    """
    Reads the temporal columns of the pyarrow.Table and
    returns a dictionary with information depending on the
    temporality_type of the data.
    """
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
    min_date = min([date for date in [start_min, stop_min] if date is not None])
    max_date = max([date for date in [start_max, stop_max] if date is not None])

    temporal_data = {
        "start": min(min_date, existing_data.get("start", min_date)),
        "latest": max(max_date, existing_data.get("latest", max_date)),
    }
    return temporal_data


def ms_to_eta(milliseconds: int) -> str:
    seconds = milliseconds / 1000
    (days, seconds) = divmod(seconds, int(24 * 3600))
    (hours, seconds) = divmod(seconds, 3600)
    (minutes, seconds) = divmod(seconds, 60)
    if days > 0:
        return (
            f"{int(days):} days, {hours:02.0f}:{minutes:02.0f}:{seconds:02.0f}"
        )
    elif hours > 0:
        return f"{hours:02.0f}:{minutes:02.0f}:{seconds:02.0f}"
    else:
        return f"{minutes:02.0f}:{seconds:02.0f}"


def validate_unit_start_stop(unit_array: list) -> bool:
    if len(unit_array) == 0:
        return True
    elif len(unit_array) == 1:
        return True
    else:
        # https://stackoverflow.com/a/325964
        _, start_a, end_a = unit_array[0]
        for _, start_b, end_b in unit_array[1:]:
            if start_a <= end_b and end_a >= start_b:
                raise ValidationError("Overlap for dates", errors=[])
        return validate_unit_start_stop(unit_array[1:])


def _no_overlapping_timespans_check(
    row_count: int, file_size: int, conn: sqlite3.Connection
) -> bool:
    logger.info("Validating start and stop dates ...")
    logger.info("Creating index ...")
    start_ms = current_milli_time()
    conn.execute("CREATE INDEX IF NOT EXISTS index_unit_id ON dataset(unit_id)")
    conn.commit()
    spent_ms = current_milli_time() - start_ms
    logger.info(
        f"Creating index ... Done in {spent_ms:_} ms aka {ms_to_eta(spent_ms)}"
    )
    start_ms = current_milli_time()
    last_log = -1
    cursor = conn.cursor()
    process = psutil.Process(os.getpid())
    try:
        cursor.execute(
            "SELECT unit_id, start_day, stop_day FROM dataset ORDER BY unit_id"
        )
        processed_rows = 0
        curr_unit = []
        while True:
            res = cursor.fetchone()
            if res is None:
                if len(curr_unit) != 0:
                    validate_unit_start_stop(curr_unit)
                break

            processed_rows += 1
            lst_log = log_time()
            if lst_log == last_log and processed_rows != row_count:
                pass
            elif processed_rows != 1:
                last_log = lst_log
                spent_ms_so_far = current_milli_time() - start_ms
                ms_per_row = spent_ms_so_far / processed_rows
                remaining_ms = ms_per_row * (row_count - processed_rows)
                row_count_len = len(f"{row_count:_}")
                processed_rows_str = f"{processed_rows:_}".rjust(row_count_len)
                percent_done = (processed_rows * 100) / row_count
                percent_done_str = f"{percent_done:.1f}".rjust(len("100.0"))
                mb_per_s = (
                    (file_size * (processed_rows / row_count)) / 1024 / 1024
                ) / (max(spent_ms_so_far, 1) / 1000)
                mb_per_s_str = f"{mb_per_s:.1f}".rjust(len("100.0"))
                mem = process.memory_info()[0] // 1024 // 1024
                mem_str = f"{mem}".rjust(len("123"))

                logger.info(
                    f"Validated {processed_rows_str} rows, "
                    + f"{mem_str} MB mem used, "
                    + f"{mb_per_s_str} MB/s, "
                    + f"{percent_done_str} % done. "
                    + f"ETA: {ms_to_eta(int(remaining_ms))}"
                )

            if len(curr_unit) == 0:
                # first unit_id
                curr_unit.append(res)
            elif res[0] == curr_unit[0][0]:
                # same unit_id
                curr_unit.append(res)
            else:
                # different unit_id.
                # first validate:
                validate_unit_start_stop(curr_unit)
                # begin with new unit id:
                curr_unit = [res]

        spent_ms = current_milli_time() - start_ms
        mb_per_s = (
            (file_size * (processed_rows / row_count)) / 1024 / 1024
        ) / (max(spent_ms, 1) / 1000)
        logger.info(
            f"Validating start and stop dates ... Done in {ms_to_eta(spent_ms)}"
        )
        logger.info(
            f"Validating start and stop dates speed: {mb_per_s:.1f} MB/s"
        )
        return True
    finally:
        cursor.close()


def _get_error_list(invalid_rows: pyarrow.Table, message: str) -> list[str]:
    invalid_identifiers = (
        invalid_rows.column("unit_id").slice(0, 50).to_pylist()
    )
    return [
        f"{message} for row with identifier: {identifier}"
        for identifier in invalid_identifiers
    ]


def _valid_unit_id_check(tbl: pyarrow.Table) -> None:
    is_null_filter = dataset.field("unit_id").is_null()
    is_empty_string_filter = dataset.field("unit_id") == ""
    invalid_rows_filter = is_null_filter | is_empty_string_filter
    invalid_rows = tbl.filter(invalid_rows_filter)

    if len(invalid_rows) > 0:
        raise ValidationError(
            "#1 column",
            errors=_get_error_list(
                invalid_rows, "Invalid identifier in #1 column"
            ),
        )


def _valid_value_column_check(
    tbl: pyarrow.Table,
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
    invalid_rows = tbl.filter(invalid_rows_filter)

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
        invalid_rows = tbl.filter(invalid_code_filter)
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


def _accumulated_temporal_variables_check(tbl: pyarrow.Table) -> None:
    start_is_null_filter = dataset.field("start_epoch_days").is_null()
    stop_is_null_filter = dataset.field("stop_epoch_days").is_null()
    start_be_stop_filter = dataset.field("start_epoch_days") >= dataset.field(
        "stop_epoch_days"
    )

    invalid_rows = tbl.filter(
        start_is_null_filter | stop_is_null_filter | start_be_stop_filter
    )

    if len(invalid_rows) > 0:
        raise ValidationError(
            "#3 and #4 columns",
            errors=_get_error_list(
                invalid_rows, "Invalid #3 and/or #4 columns"
            ),
        )


def csv_to_parquet(
    identifier_data_type: str,
    measure_data_type: str,
    code_list: Union[List, None],
    sentinel_list: Union[List, None],
    file_size: int,
    row_count: int,
    reader: pyarrow.csv.CSVStreamingReader,
    writer: parquet.ParquetWriter,
    conn: sqlite3.Connection,
) -> Dict[str, int]:
    start_time = current_milli_time()
    temporal_data = {}
    processed_rows = 0
    last_log = -1
    max_row_count_str = f"{row_count:_}"
    process = psutil.Process(os.getpid())
    logger.info("Writing parquet file ...")
    while True:
        try:
            batch = reader.read_next_batch()
        except StopIteration:
            break
        if identifier_data_type == "STRING":
            unit_id = compute.utf8_trim(batch["unit_id"], " ")
        else:
            unit_id = batch["unit_id"]
        value = ru.sanitize_value2(batch, measure_data_type)
        epoch_start = ru.cast_to_epoch_date2(batch, "start")
        epoch_stop = ru.cast_to_epoch_date2(batch, "stop")
        columns = [unit_id, value, epoch_start, epoch_stop]
        column_names = [
            "unit_id",
            "value",
            "start_epoch_days",
            "stop_epoch_days",
            "start_year",
        ]
        columns.append(ru.generate_start_year2(batch))
        tbl = pyarrow.Table.from_arrays(columns, column_names)
        temporal_data = get_temporal_data2(tbl, temporal_data)
        processed_rows += len(tbl)

        _valid_unit_id_check(tbl)
        _valid_value_column_check(
            tbl, measure_data_type, code_list, sentinel_list
        )
        _accumulated_temporal_variables_check(tbl)

        sql_batch = []
        for idx in range(len(tbl)):
            unit_id_one = unit_id[idx].as_py()
            start_day = epoch_start[idx].as_py()
            stop_day = epoch_stop[idx].as_py()
            sql_batch.append((str(unit_id_one), start_day, stop_day))
        conn.executemany(
            """INSERT INTO dataset (unit_id, start_day, stop_day)
               VALUES (?, ?, ?)""",
            sql_batch,
        )
        conn.commit()

        batch_to_write = pyarrow.RecordBatch.from_arrays(columns, column_names)
        writer.write_batch(batch_to_write)

        if last_log == log_time() and processed_rows != row_count:
            pass
        else:
            last_log = log_time()
            spent_ms_so_far = current_milli_time() - start_time
            ms_per_row = spent_ms_so_far / processed_rows
            remaining_ms = ms_per_row * (row_count - processed_rows)
            mb_per_s = (
                (file_size * (processed_rows / row_count)) / 1024 / 1024
            ) / (max(spent_ms_so_far, 1) / 1000)
            processed_rows_str = f"{processed_rows:_}".rjust(
                len(max_row_count_str)
            )
            percent_done = (processed_rows * 100) / row_count
            percent_done_str = f"{percent_done:.1f}".rjust(len("100.0"))
            mb_per_s_str = f"{mb_per_s:.1f}".rjust(len("100.0"))
            mem = process.memory_info()[0] // 1024 // 1024
            mem_str = f"{mem}".rjust(len("123"))
            logger.info(
                f"Wrote {processed_rows_str} rows, "
                + f"{mem_str} MB mem used, "
                + f"{mb_per_s_str} MB/s, "
                + f"{percent_done_str} % done. "
                + f"ETA: {ms_to_eta(int(remaining_ms))}"
            )

    spent_ms = current_milli_time() - start_time
    logger.info(f"Writing parquet file ... Done in {ms_to_eta(spent_ms)}")
    return temporal_data


def read_and_sanitize_csv2(
    input_data_path: Path,
    output_data_path: Path,
    identifier_data_type: str,
    measure_data_type: str,
    temporality_type: str,
    code_list: Union[List, None],
    sentinel_list: Union[List, None],
) -> dict[str, int]:
    """
    Reads a csv file to a pyarrow table. Sanitizes values and
    ensures the input csv data follows the requirements for the
    microdata data model.
    """
    start_time = current_milli_time()
    logger.info(f"identifier_data_type: {identifier_data_type}")
    logger.info(f"measure_data_type: {measure_data_type}")
    logger.info(f"temporality_type: {temporality_type}")
    row_count = ru.get_row_count(input_data_path)

    logger.info(f"row_count: {row_count:_}")
    file_size = input_data_path.stat().st_size
    schema = pyarrow.schema(
        [
            (
                "unit_id",
                ru.microdata_data_type_to_pyarrow(identifier_data_type),
            ),
            ("value", ru.microdata_data_type_to_pyarrow(measure_data_type)),
            ("start_epoch_days", pyarrow.int16()),
            ("stop_epoch_days", pyarrow.int16()),
            ("start_year", pyarrow.string()),
        ]
    )

    if os.path.exists("tmp.db"):
        os.remove("tmp.db")

    try:
        with sqlite3.connect("tmp.db", autocommit=False) as conn:
            conn.execute(
                """CREATE TABLE dataset(unit_id VARCHAR,
                                        start_day INTEGER,
                                        stop_day INTEGER)"""
            )
            conn.commit()
            with parquet.ParquetWriter(output_data_path, schema) as writer:
                with csv.open_csv(
                    input_data_path,
                    parse_options=csv.ParseOptions(delimiter=";"),
                    read_options=ru.get_csv_read_options(),
                    convert_options=ru.get_csv_convert_options(
                        identifier_data_type, measure_data_type
                    ),
                ) as reader:
                    temporal_data = csv_to_parquet(
                        identifier_data_type,
                        measure_data_type,
                        code_list,
                        sentinel_list,
                        file_size,
                        row_count,
                        reader,
                        writer,
                        conn,
                    )
            logger.info("Done writing parquet file!")

            _no_overlapping_timespans_check(row_count, file_size, conn)
    finally:
        try:
            os.remove("tmp.db")
        except Exception:
            pass
    spent_ms = current_milli_time() - start_time
    mb_per_s = (file_size / 1024 / 1024) / (spent_ms / 1000)
    logger.debug(f"read_and_sanitize_csv2 spent: {spent_ms:_} ms")
    logger.debug(f"read_and_sanitize_csv2 speed: {mb_per_s:.1f} MB/s")
    return temporal_data
