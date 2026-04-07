import multiprocessing
import os
import sqlite3

import psutil

from microdata_tools.validation.exceptions import ValidationError
from microdata_tools.validation.steps import utils

_is_error: multiprocessing.Event
_report_q: multiprocessing.SimpleQueue


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


def init_worker(
    is_error: multiprocessing.Event,
    mem_pid_q: multiprocessing.SimpleQueue,
    report_q: multiprocessing.SimpleQueue,
) -> None:
    global _is_error
    global _report_q
    _is_error = is_error
    _report_q = report_q
    mem_pid_q.put(os.getpid())


def no_overlapping_timespans_check_worker(offset: int, limit: int) -> int:
    global _report_q
    global _is_error
    return _no_overlapping_timespans_check_worker_inner(
        _is_error, _report_q, offset, limit
    )


def _no_overlapping_timespans_check_worker_inner(
    is_error: multiprocessing.Event,
    report_q: multiprocessing.SimpleQueue,
    offset: int,
    limit: int,
) -> int:
    row_count = limit
    with sqlite3.connect("tmp.db", autocommit=False) as conn:
        last_report = -1
        cursor = conn.cursor()
        process = psutil.Process(os.getpid())
        try:
            cursor.execute(
                "SELECT unit_id, start_epoch_days, stop_epoch_days "
                + "FROM dataset "
                + "ORDER BY unit_id LIMIT ? OFFSET ?",
                (limit, offset),
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
                elif processed_rows != 1:
                    last_report = lst_report
                    report_q.put(
                        {
                            "pid": os.getpid(),
                            "processed_rows": processed_rows,
                            "mem": process.memory_info()[1] // 1024 // 1024,
                        }
                    )
            return processed_rows
        finally:
            cursor.close()
