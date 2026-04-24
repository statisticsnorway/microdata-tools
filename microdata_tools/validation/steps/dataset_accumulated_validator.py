# pyright: reportAttributeAccessIssue=false
import logging
import multiprocessing
import os.path
import sqlite3
from pathlib import Path
from typing import Dict, List, Union

import adbc_driver_sqlite.dbapi
import psutil
import pyarrow
from pyarrow import compute, csv, dataset, parquet

import microdata_tools.validation.steps.reader_utils as ru
from microdata_tools.validation.exceptions import ValidationError
from microdata_tools.validation.steps import overlap_validator, utils

logger = logging.getLogger()


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
        logger.error(f"Error count: {len(invalid_rows)}")
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

    invalid_rows1 = tbl.filter(
        start_is_null_filter | stop_is_null_filter | start_be_stop_filter
    )

    if len(invalid_rows1) > 0:
        invalid_rows2 = tbl.filter(start_is_null_filter)
        if len(invalid_rows2) > 0:
            logger.error(f"Error count start_is_null: {len(invalid_rows2)}")

        invalid_rows3 = tbl.filter(stop_is_null_filter)
        if len(invalid_rows3) > 0:
            logger.error(f"Error count stop_is_null: {len(invalid_rows3)}")

        invalid_rows4 = tbl.filter(start_be_stop_filter)
        if len(invalid_rows4) > 0:
            logger.error(f"Error count start_be_stop: {len(invalid_rows4)}")

        logger.error(f"Error count: {len(invalid_rows1)} of total {len(tbl)}")
        raise ValidationError(
            "#3 and #4 columns",
            errors=_get_error_list(
                invalid_rows1, "Invalid #3 and/or #4 columns"
            ),
        )


def _csv_stream_to_sqlite(
    identifier_data_type: str,
    measure_data_type: str,
    code_list: Union[List, None],
    sentinel_list: Union[List, None],
    file_size: int,
    row_count: int,
    reader: pyarrow.csv.CSVStreamingReader,
    conn: sqlite3.Connection,
    writer: pyarrow.parquet.ParquetWriter,
) -> Dict[str, int]:
    logger.debug("Streaming to sqlite and validating")
    start_time = utils.current_milli_time()
    temporal_data = {}
    processed_rows = 0
    last_log = -1
    process = psutil.Process(os.getpid())
    cursor = conn.cursor()
    while True:
        try:
            batch = reader.read_next_batch()
        except StopIteration:
            cursor.close()
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
        writer.write_table(tbl)

        _valid_unit_id_check(tbl)
        _valid_value_column_check(
            tbl, measure_data_type, code_list, sentinel_list
        )
        _accumulated_temporal_variables_check(tbl)

        processed_rows += len(tbl)

        column_names2 = [
            "unit_id",
            "start_epoch_days",
            "stop_epoch_days",
        ]
        columns2 = [unit_id, epoch_start, epoch_stop]
        cursor.adbc_ingest(
            "dataset",
            pyarrow.Table.from_arrays(columns2, column_names2),
            mode="append",
        )
        conn.commit()
        temporal_data = get_temporal_data2(tbl, temporal_data)

        if last_log == utils.log_time() and processed_rows != row_count:
            pass
        else:
            last_log = utils.log_time()
            _stream_show_progress(
                file_size,
                process.memory_info().rss // 1000 // 1000,
                processed_rows,
                row_count,
                start_time,
            )

    return temporal_data


def _stream_show_progress(
    file_size: int,
    mem_mb: int,
    processed_rows: int,
    row_count: int,
    start_time: int,
) -> None:
    spent_ms_so_far = utils.current_milli_time() - start_time
    ms_per_row = spent_ms_so_far / processed_rows
    remaining_ms = ms_per_row * (row_count - processed_rows)
    mb_per_s = ((file_size * (processed_rows / row_count)) / 1024 / 1024) / (
        max(spent_ms_so_far, 1) / 1000
    )
    max_row_count_str = f"{row_count:_}"
    processed_rows_str = f"{processed_rows:_}".rjust(len(max_row_count_str))
    percent_done = (processed_rows * 100) / row_count
    percent_done_str = f"{percent_done:.1f}".rjust(len("100.0"))
    mb_per_s_str = f"{mb_per_s:.1f}".rjust(len("100.0"))
    mem_str = f"{mem_mb:_}".rjust(len("123_123"))
    logger.info(
        f"Validated and prepared {processed_rows_str} rows, "
        + f"{mem_str} MiB RSS mem, "
        + f"{mb_per_s_str} MB/s, "
        + f"{percent_done_str} % done. "
        + f"ETA: {utils.ms_to_eta(int(remaining_ms))}"
    )


def sanitize_and_validate_csv(
    mem_pid_q: Union[multiprocessing.SimpleQueue, None],
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
    start_ms = utils.current_milli_time()
    row_count = ru.get_row_count(input_data_path)
    logger.info(f"identifier_data_type: {identifier_data_type}")
    logger.info(f"measure_data_type: {measure_data_type}")
    logger.info(f"temporality_type: {temporality_type}")
    logger.info(f"row_count: {row_count:_}")
    file_size = input_data_path.stat().st_size

    if os.path.exists("tmp.db"):
        os.remove("tmp.db")

    try:
        logger.info("Validating and preparing data ...")
        temporal_data = _populate_sqlite(
            code_list,
            file_size,
            identifier_data_type,
            input_data_path,
            measure_data_type,
            output_data_path,
            row_count,
            sentinel_list,
        )
        spent_ms1 = utils.current_milli_time() - start_ms
        mb_per_s1 = (file_size / 1024 / 1024) / (spent_ms1 / 1000)
        logger.info(
            f"Validating and preparing data ... Done in {spent_ms1:_} ms aka "
            + f"{utils.ms_to_eta(spent_ms1)}"
        )
        logger.debug(
            f"Validating and preparing data speed: {mb_per_s1:.1f} MB/s"
        )

        start_ms2 = utils.current_milli_time()
        logger.info("Creating index ...")
        _create_index()
        spent_ms2 = utils.current_milli_time() - start_ms2
        mb_per_s2 = (file_size / 1024 / 1024) / (spent_ms2 / 1000)
        logger.info(
            f"Creating index ... Done in {spent_ms2:_} ms aka "
            + f"{utils.ms_to_eta(spent_ms2)}"
        )
        logger.debug(f"Creating index speed: {mb_per_s2:.1f} MB/s")

        start_ms3 = utils.current_milli_time()
        overlap_validator.check_no_overlaps(file_size, mem_pid_q, row_count)
        spent_ms3 = utils.current_milli_time() - start_ms3
        mb_per_s3 = (file_size / 1024 / 1024) / (max(spent_ms3, 1) / 1000)
        logger.info(f"Validated rows speed: {mb_per_s3:.1f} MB/s")
        logger.info(
            f"Validated rows done in {spent_ms3:_} ms aka "
            + f"{utils.ms_to_eta(spent_ms3)}"
        )

        total_ms = utils.current_milli_time() - start_ms
        mb_per_s4 = (file_size / 1024 / 1024) / (total_ms / 1000)

        logger.info("*" * 80)
        logger.info("Summary:")
        logger.info(f"Row count: {row_count:_}")
        share1 = 100 * spent_ms1 / total_ms
        logger.debug(
            f"Validating and preparing data: {mb_per_s1:.1f} MB/s, "
            + f"{share1:.1f} %"
        )
        share2 = 100 * spent_ms2 / total_ms
        logger.debug(f"Creating index: {mb_per_s2:.1f} MB/s, {share2:.1f} %")
        share3 = 100 * spent_ms3 / total_ms
        logger.info(f"Validated rows: {mb_per_s3:.1f} MB/s, {share3:.1f} %")
        logger.debug(f"Total speed: {mb_per_s4:.1f} MB/s")
        logger.debug(
            f"Total spent {total_ms:_} ms aka " + f"{utils.ms_to_eta(total_ms)}"
        )
        logger.info("*" * 80)

        return temporal_data
    finally:
        try:
            os.remove("tmp.db")
        except Exception:
            pass


def _create_index() -> None:
    with sqlite3.connect("tmp.db", autocommit=True) as conn:
        conn.execute(
            "CREATE INDEX IF NOT EXISTS index_unit_id ON dataset(unit_id)"
        )


def _populate_sqlite(
    code_list: list | None,
    file_size: int,
    identifier_data_type: str,
    input_data_path: Path,
    measure_data_type: str,
    output_data_path: Path,
    row_count: int,
    sentinel_list: list | None,
) -> dict[str, int]:

    with adbc_driver_sqlite.dbapi.connect("tmp.db", autocommit=False) as conn:
        conn.execute(
            """CREATE TABLE dataset
               (
                   unit_id          VARCHAR,
                   start_epoch_days INTEGER,
                   stop_epoch_days  INTEGER
               )"""
        )
        conn.commit()
        with csv.open_csv(
            input_data_path,
            parse_options=csv.ParseOptions(delimiter=";"),
            read_options=ru.get_csv_read_options(),
            convert_options=ru.get_csv_convert_options(
                identifier_data_type, measure_data_type
            ),
        ) as reader:
            parquet_data_path = str(input_data_path)[:-4] + ".parquet"
            logger.info(f"parquet path is: {parquet_data_path}")
            schema = pyarrow.schema(
                [
                    (
                        "unit_id",
                        ru.microdata_data_type_to_pyarrow(identifier_data_type),
                    ),
                    (
                        "value",
                        ru.microdata_data_type_to_pyarrow(measure_data_type),
                    ),
                    ("start_epoch_days", pyarrow.int16()),
                    ("stop_epoch_days", pyarrow.int16()),
                    ("start_year", pyarrow.string()),
                ]
            )
            with parquet.ParquetWriter(parquet_data_path, schema) as writer:
                temporal_data = _csv_stream_to_sqlite(
                    identifier_data_type,
                    measure_data_type,
                    code_list,
                    sentinel_list,
                    file_size,
                    row_count,
                    reader,
                    conn,
                    writer,
                )
    logger.info("returning temporal_data")
    return temporal_data
