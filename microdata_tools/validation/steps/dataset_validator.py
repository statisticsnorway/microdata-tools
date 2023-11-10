from typing import List, Union
from pyarrow import dataset, compute, Table
from pyarrow.dataset import FileSystemDataset

from microdata_tools.validation.exceptions import ValidationError


def _get_error_list(invalid_rows: Table, message: str):
    invalid_identifiers = (
        invalid_rows.column("unit_id").slice(0, 5).to_pylist()
    )
    return [
        f"{message} for row with identifier: {identifier}"
        for identifier in invalid_identifiers
    ]


def _valid_value_column_check(
    data: FileSystemDataset,
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
        if sentinel_list is not None:
            unique_codes += list(
                set(
                    sentinel_list_item["code"]
                    for sentinel_list_item in sentinel_list
                )
            )
        invalid_code_filter = ~dataset.field("value").isin(unique_codes)
        invalid_rows = data.to_table(
            filter=invalid_code_filter, columns=["value"]
        )
        if len(invalid_rows) > 0:
            invalid_codes = (
                invalid_rows.column("value").slice(0, 5).to_pylist()
            )
            raise ValidationError(
                "#2 column",
                errors=[f"{code} not in code list" for code in invalid_codes],
            )


def _valid_unit_id_check(data: FileSystemDataset):
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


def _fixed_temporal_variables_check(data: FileSystemDataset):
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


def _status_temporal_variables_check(data: FileSystemDataset):
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


def _event_temporal_variables_check(data: FileSystemDataset):
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


def _accumulated_temporal_variables_check(data: FileSystemDataset):
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


def _only_unique_identifiers_check(data: FileSystemDataset):
    """
    A table with temporalityType=FIXED is only valid if all
    cells in the unit_id column are unique.
    """
    identifiers = data.to_table(columns=["unit_id"])
    identifiers = Table.from_arrays(
        [
            compute.utf8_slice_codeunits(
                identifiers["unit_id"], start=0, stop=1
            ),
            identifiers["unit_id"],
        ],
        names=["bucket", "unit_id"],
    )
    unique_buckets = compute.unique(identifiers["bucket"])
    for unique_bucket in unique_buckets:
        bucket_table = identifiers.filter(
            dataset.field("bucket") == unique_bucket
        )
        bucket_row_count = len(bucket_table)
        unique_identifiers_count = len(compute.unique(bucket_table["unit_id"]))
        if unique_identifiers_count != bucket_row_count:
            raise ValidationError(
                "#1 column",
                errors=["Duplicate identifiers in #1 column"],
            )


def _status_uniquesness_check(data: FileSystemDataset):
    """
    A table with temporalityType=STATUS is valid only if all
    cells in the unit_id column are unique per status date.
    """
    all_status_dates = data.to_table(columns=["start_epoch_days"])
    unique_status_dates = compute.unique(all_status_dates["start_epoch_days"])
    for status_date in unique_status_dates:
        status_table = data.to_table(
            columns=["unit_id"],
            filter=dataset.field("start_epoch_days") == status_date,
        )
        unique_identifiers = compute.unique(status_table["unit_id"])
        if len(unique_identifiers) != len(status_table):
            raise ValidationError(
                "#1, #3 and #4 columns",
                errors=[
                    "Same unit_id (#1 Column) has duplicate dates "
                    "(#3 and #4 column)"
                ],
            )


def _no_overlapping_timespans_check(data: FileSystemDataset):
    """
    A table with temporalityType=(EVENT|ACCUMULATED) is valid
    only if all rows for a given identifier contains no overlapping
    timespans in the start_epoch_days and stop_epoch_days columns.
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

    identifiers = data.to_table(columns=["unit_id"])
    unique_identifiers = compute.unique(identifiers["unit_id"])
    for identifier_batch in batch(unique_identifiers, 500_000):
        identifier_time_spans = data.to_table(
            filter=dataset.field("unit_id").isin(identifier_batch),
            columns=["unit_id", "start_epoch_days", "stop_epoch_days"],
        )
        identifier_time_spans = identifier_time_spans.sort_by(
            [("start_epoch_days", "ascending")]
        )
        identifier_time_spans = identifier_time_spans.group_by(
            "unit_id"
        ).aggregate(
            [("start_epoch_days", "list"), ("stop_epoch_days", "list")]
        )
        for i in range(len(identifier_time_spans)):
            if find_overlap(
                identifier_time_spans["start_epoch_days_list"][i].as_py(),
                identifier_time_spans["stop_epoch_days_list"][i].as_py(),
            ):
                raise ValidationError(
                    "#1, #3 and #4 columns",
                    errors=[
                        "Invalid overlapping timespans for identifier"
                        f' "{identifier_time_spans["unit_id"][i]}"'
                    ],
                )


def validate_dataset(
    data: FileSystemDataset,
    measure_data_type: str,
    code_list: Union[List, None],
    sentinel_list: Union[List, None],
    temporality_type: str,
) -> None:
    _valid_unit_id_check(data)
    _valid_value_column_check(
        data, measure_data_type, code_list, sentinel_list
    )
    if temporality_type == "FIXED":
        _fixed_temporal_variables_check(data)
        _only_unique_identifiers_check(data)
    elif temporality_type == "STATUS":
        _status_temporal_variables_check(data)
        _status_uniquesness_check(data)
    elif temporality_type == "ACCUMULATED":
        _accumulated_temporal_variables_check(data)
        _no_overlapping_timespans_check(data)
    elif temporality_type == "EVENT":
        _event_temporal_variables_check(data)
        _no_overlapping_timespans_check(data)
