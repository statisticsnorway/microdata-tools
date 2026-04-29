import logging
import os
import sqlite3

import psutil

from microdata_tools.validation.exceptions import ValidationError
from microdata_tools.validation.steps import config, utils

logger = logging.getLogger()


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


def _show_progress_validation(
    file_size: int,
    mem: int,
    processed_rows: int,
    row_count: int,
    spent_ms_so_far: int,
) -> None:
    ms_per_row = spent_ms_so_far / max(1, processed_rows)
    remaining_ms = ms_per_row * (row_count - processed_rows)
    mb_per_s = ((file_size * (processed_rows / row_count)) / 1024 / 1024) / (
        max(spent_ms_so_far, 1) / 1000
    )
    processed_rows_str = f"{processed_rows:_}".rjust(len(f"{row_count:_}"))
    percent_done = (processed_rows * 100) / row_count
    percent_done_str = f"{percent_done:.1f}".rjust(len("100.0"))
    mb_per_s_str = f"{mb_per_s:.1f}".rjust(len("100.0"))
    mem_str = f"{mem}".rjust(len("1234"))
    logger.info(
        f"Overlap validated {processed_rows_str} rows, "
        + f"{mem_str} RSS MiB mem used, "
        + f"{mb_per_s_str} MB/s, "
        + f"{percent_done_str} % done. "
        + f"ETA: {utils.ms_to_eta(int(remaining_ms))}"
    )


def no_overlapping_timespans_check_worker(
    file_size: int,
    row_count: int,
) -> int:
    with sqlite3.connect(config.tmp_db_file(), autocommit=False) as conn:
        last_report = -1
        cursor = conn.cursor()
        process = psutil.Process(os.getpid())
        start_time = utils.current_milli_time()
        try:
            cursor.execute(
                "SELECT unit_id, start_epoch_days, stop_epoch_days "
                + "FROM dataset "
                + "ORDER BY unit_id"
            )
            processed_rows = 0
            curr_unit = []
            while True:
                res = cursor.fetchone()
                if res is None:
                    if len(curr_unit) != 0:
                        validate_unit_start_stop(curr_unit)
                        curr_unit_id = curr_unit[0][0]
                        cursor.execute(
                            "SELECT unit_id, start_epoch_days, stop_epoch_days "
                            + "FROM DATASET WHERE unit_id = ?",
                            (curr_unit_id,),
                        )
                        curr_unit_2 = cursor.fetchall()
                        assert len(curr_unit_2) >= 1
                        validate_unit_start_stop(curr_unit_2)
                    break

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

                processed_rows += 1
                lst_report = utils.log_time()
                if lst_report == last_report and processed_rows != row_count:
                    pass
                else:
                    last_report = lst_report
                    _show_progress_validation(
                        file_size,
                        process.memory_info().rss // 1000 // 1000,
                        processed_rows,
                        row_count,
                        utils.current_milli_time() - start_time,
                    )

            return processed_rows
        finally:
            cursor.close()
