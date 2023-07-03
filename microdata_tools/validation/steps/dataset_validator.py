from typing import List, Union
from pyarrow import dataset, compute, Table


def valid_value_column_check(
    parquet_path: str,
    data_type: str,
    code_list: Union[list, None],
    sentinel_list: Union[List, None],
):
    """
    Any given cell in the value column is valid only if:
    * The cell contains a a valid non-null value
    * The cell does not contain an empty string if data_type is STRING
    * The value is present in the code_list if supplied
    """
    is_null_filter = dataset.field("measure").is_null()
    is_empty_string_filter = dataset.field("measure") == ""
    invalid_rows_filter = (
        (is_null_filter | is_empty_string_filter)
        if data_type == "STRING"
        else is_null_filter
    )
    invalid_rows = dataset.dataset(parquet_path).to_table(
        filter=invalid_rows_filter, columns=["identifier"]
    )
    if len(invalid_rows) > 0:
        raise ValueError("valid_value_column_check")

    if code_list:
        unique_codes = list(
            set(code_list_item["code"] for code_list_item in code_list)
        )
        if sentinel_list is not None:
            unique_codes += list(
                set(
                    sentinel_list_item["code"]
                    for sentinel_list_item in sentinel_list
                )
            )
        invalid_code_filter = ~dataset.field("measure").isin(unique_codes)
        invalid_rows = dataset.dataset(parquet_path).to_table(
            filter=invalid_code_filter, columns=["measure"]
        )
        if len(invalid_rows) > 0:
            raise ValueError("invalid code list rows")


def valid_unit_id_check(parquet_path: str):
    """
    Any given cell in the unit_id column is valid only if:
    * The cell contains a a valid non-null value
    * The cell does not contain an empty string
    """
    is_null_filter = dataset.field("identifier").is_null()
    is_empty_string_filter = dataset.field("identifier") == ""
    invalid_rows_filter = is_null_filter | is_empty_string_filter
    invalid_rows = dataset.dataset(parquet_path).to_table(
        filter=invalid_rows_filter, columns=["identifier"]
    )
    if len(invalid_rows) > 0:
        raise ValueError("valid_unit_id_column_check")


def fixed_temporal_variables_check(parquet_path: str):
    """
    Any given row in a table with temporalityType=FIXED is valid only if:
    * The epoch_start column contains null (empty)
    * The epoch_stop column contains a non-null value (int32)
    """
    start_is_valid_filter = dataset.field("epoch_start").is_valid()
    stop_is_null_filter = dataset.field("epoch_stop").is_null()
    invalid_rows = dataset.dataset(parquet_path).to_table(
        filter=start_is_valid_filter | stop_is_null_filter,
        columns=["identifier"],
    )
    if len(invalid_rows) > 0:
        raise ValueError("valid_unit_id_column_check")


def status_temporal_variables_check(parquet_path: str):
    """
    Any given row in a table with temporalityType=STATUS is valid only if:
    * The epoch_start column contains a non-null value (int32)
    * The epoch_stop column contains a non-null value (int32)
    * The epoch_start and epoch_stop columns contain the same value
      for any given row
    """
    invalid_rows = dataset.dataset(parquet_path).to_table(
        filter=(
            dataset.field("epoch_stop").is_null()
            | dataset.field("epoch_start").is_null()
        ),
        columns=["identifier"],
    )
    if len(invalid_rows) > 0:
        raise ValueError(
            "No row can have an empty start or stop column with temporalityType: STATUS"
        )
    invalid_rows = dataset.dataset(parquet_path).to_table(
        filter=dataset.field("epoch_start") != dataset.field("epoch_stop"),
        columns=["identifier"],
    )
    if len(invalid_rows) > 0:
        raise ValueError("start did not equal stop")


def event_temporal_variables_check(parquet_path: str):
    """
    Any given row in a table with temporalityType=EVENT is valid only if:
    * The epoch_start column contains a non-null value (int32)
    * The epoch_stop is either a non-null value bigger than
      epoch_start (int32), or null (empty)
    """
    start_is_null_filter = dataset.field("epoch_start").is_null()
    start_be_stop_filter = dataset.field("epoch_start") >= dataset.field(
        "epoch_stop"
    )  # If epoch_stop is null this test will be ignored by pyarrow
    invalid_rows = dataset.dataset(parquet_path).to_table(
        filter=(start_is_null_filter | start_be_stop_filter),
        columns=["identifier"],
    )
    if len(invalid_rows) > 0:
        raise ValueError(f"valid_event_temporal_columns_check {invalid_rows}")


def accumulated_temporal_variables_check(parquet_path: str):
    """
    Any given row in a table with temporalityType=EVENT is valid only if:
    * The epoch_start column contains a non-null value (int32)
    * The epoch_stop is non-null (int32) value bigger than epoch_start
    """
    start_is_null_filter = dataset.field("epoch_start").is_null()
    stop_is_null_filter = dataset.field("epoch_stop").is_null()
    start_be_stop_filter = dataset.field("epoch_start") >= dataset.field(
        "epoch_stop"
    )
    invalid_rows = dataset.dataset(parquet_path).to_table(
        filter=(
            start_is_null_filter | stop_is_null_filter | start_be_stop_filter
        ),
        columns=["identifier"],
    )
    if len(invalid_rows) > 0:
        raise ValueError(
            f"valid_accumulated_temporal_columns_check {invalid_rows}"
        )


def only_unique_identifiers_check(parquet_path: str):
    """
    A table with temporalityType=FIXED is only valid if all
    cells in the unit_id column are unique.
    """
    identifiers = dataset.dataset(parquet_path).to_table(
        columns=["identifier"],
    )
    identifiers = Table.from_arrays(
        [
            compute.utf8_slice_codeunits(
                identifiers["identifier"], start=0, stop=1
            ),
            identifiers["identifier"],
        ],
        names=["bucket", "identifier"],
    )
    unique_buckets = compute.unique(identifiers["bucket"])
    for unique_bucket in unique_buckets:
        bucket_table = identifiers.filter(
            dataset.field("bucket") == unique_bucket
        )
        bucket_row_count = len(bucket_table)
        unique_identifiers_count = len(
            compute.unique(bucket_table["identifier"])
        )
        if unique_identifiers_count != bucket_row_count:
            raise ValueError("identifier diff")


def status_uniquesness_check(parquet_path: str):
    """
    A table with temporalityType=STATUS is valid only if all
    cells in the unit_id column are unique per status date.
    """
    all_status_dates = dataset.dataset(parquet_path).to_table(
        columns=["epoch_start"],
    )
    unique_status_dates = compute.unique(all_status_dates["epoch_start"])
    for status_date in unique_status_dates:
        status_table = dataset.dataset(parquet_path).to_table(
            columns=["identifier"],
            filter=dataset.field("epoch_start") == status_date,
        )
        unique_identifiers = compute.unique(status_table["identifier"])
        if len(unique_identifiers) != len(status_table):
            raise ValueError("status uniqueness fail")


def no_overlapping_timespans_check(parquet_path: str):
    """
    A table with temporalityType=(EVENT|ACCUMULATED) is valid
    only if all rows for a given identifier contains no overlapping
    timespans in the epoch_start and epoch_stop columns.
    """

    def find_overlap(start_list, stop_list):
        """
        Looks for overlapping timespans where each timespan
        is defined by a start_date at an index from the start_list,
        and a stop_date at the same index from the stop_list.
        """
        for i in range(len(start_list) - 1):
            if stop_list[i] is None:
                return True
            if stop_list[i] > start_list[i + 1]:
                return True
        return False

    def batch(iterable, batch_size):
        for index in range(0, len(iterable), batch_size):
            yield iterable[index : index + batch_size]

    identifiers = dataset.dataset(parquet_path).to_table(
        columns=["identifier"],
    )
    unique_identifiers = compute.unique(identifiers["identifier"])
    for identifier_batch in batch(unique_identifiers, 5_000_000):
        identifier_time_spans = dataset.dataset(parquet_path).to_table(
            filter=dataset.field("identifier").isin(identifier_batch),
            columns=["identifier", "epoch_start", "epoch_stop"],
        )
        identifier_time_spans = identifier_time_spans.sort_by(
            [("epoch_start", "ascending")]
        )
        identifier_time_spans = identifier_time_spans.group_by(
            "identifier"
        ).aggregate([("epoch_start", "list"), ("epoch_stop", "list")])
        for i in range(len(identifier_time_spans)):
            if find_overlap(
                identifier_time_spans["epoch_start_list"][i].as_py(),
                identifier_time_spans["epoch_stop_list"][i].as_py(),
            ):
                raise ValueError("TODO")


def validate_dataset(
    parquet_path: str,
    measure_data_type: str,
    code_list: Union[List, None],
    sentinel_list: Union[List, None],
    temporality_type: str,
) -> None:
    valid_unit_id_check(parquet_path)
    valid_value_column_check(
        parquet_path, measure_data_type, code_list, sentinel_list
    )
    match temporality_type:
        case "FIXED":
            fixed_temporal_variables_check(parquet_path)
            only_unique_identifiers_check(parquet_path)
        case "STATUS":
            status_temporal_variables_check(parquet_path)
            status_uniquesness_check(parquet_path)
        case "ACCUMULATED":
            accumulated_temporal_variables_check(parquet_path)
            no_overlapping_timespans_check(parquet_path)
        case "EVENT":
            event_temporal_variables_check(parquet_path)
            no_overlapping_timespans_check(parquet_path)
