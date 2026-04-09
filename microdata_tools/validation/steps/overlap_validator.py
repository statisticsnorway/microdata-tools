import logging
import multiprocessing
import time
from concurrent.futures import Future, ProcessPoolExecutor
from typing import Any

from microdata_tools.validation.steps import overlap_validator_worker, utils

logger = logging.getLogger()

_is_error: multiprocessing.Event
_report_q: multiprocessing.SimpleQueue


def _launch_jobs(
    chunk_size: int, pool: ProcessPoolExecutor, row_count: int
) -> list[Any]:
    jobs = []
    s = 0
    while s <= row_count:
        job = pool.submit(
            overlap_validator_worker.no_overlapping_timespans_check_worker,
            s,
            chunk_size,
        )
        jobs.append(job)
        s += chunk_size
    logger.info(f"Spawned {len(jobs):_} workers")
    return jobs


def _summarize_stats(stats: dict[Any, Any]) -> tuple[int, int]:
    mem = 0
    processed_rows = 0

    for pid in stats:
        stat = stats[pid]
        mem += stat["mem"]
        processed_rows += stat["processed_rows"]
    return mem, processed_rows


def _collect_stats(
    report_queue: multiprocessing.SimpleQueue, stats: dict[Any, Any]
) -> None:
    while not report_queue.empty():
        report = report_queue.get()
        pid = report["pid"]
        stats[pid] = {
            "processed_rows": report["processed_rows"],
            "mem": report["mem"],
        }


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
        f"Validated {processed_rows_str} rows, "
        + f"{mem_str} RSS MiB mem used, "
        + f"{mb_per_s_str} MB/s, "
        + f"{percent_done_str} % done. "
        + f"ETA: {utils.ms_to_eta(int(remaining_ms))}"
    )


def _jobs_done(jobs: list[Future]) -> bool:
    for job2 in jobs:
        if job2.done():
            continue
        else:
            return False
    return True


def _wait_jobs_report_progress(
    file_size: int,
    jobs: list[Any],
    is_error: multiprocessing.Event,
    report_queue: multiprocessing.SimpleQueue,
    row_count: int,
    start_time: int,
) -> None:
    stats = {}
    last_log = -1
    while not _jobs_done(jobs):
        _collect_stats(report_queue, stats)
        mem, processed_rows = _summarize_stats(stats)

        if processed_rows == 0:
            pass
        elif processed_rows < row_count * 0.01:
            pass
        elif processed_rows == row_count:
            pass
        elif last_log == utils.log_time():
            pass
        else:
            last_log = utils.log_time()
            _show_progress_validation(
                file_size,
                mem,
                processed_rows,
                row_count,
                utils.current_milli_time() - start_time,
            )
        time.sleep(0.016)

    assert _jobs_done(jobs)

    if is_error.is_set():
        pass
    else:
        _collect_stats(report_queue, stats)
        mem, _ = _summarize_stats(stats)
        processed_rows = sum([job.result() for job in jobs])
        _show_progress_validation(
            file_size,
            mem,
            processed_rows,
            row_count,
            utils.current_milli_time() - start_time,
        )


def check_no_overlaps(
    file_size: int, mem_pid_q: multiprocessing.SimpleQueue, row_count: int
) -> None:
    worker_count = multiprocessing.cpu_count()
    chunk_size = (row_count // worker_count) + 1
    mp_context = multiprocessing.get_context("spawn")
    is_error = mp_context.Event()
    report_queue = mp_context.SimpleQueue()
    start_time = utils.current_milli_time()
    with ProcessPoolExecutor(
        worker_count,
        mp_context=mp_context,
        initializer=overlap_validator_worker.init_worker,
        initargs=(is_error, mem_pid_q, report_queue),
    ) as pool:
        jobs = _launch_jobs(chunk_size, pool, row_count)

        _wait_jobs_report_progress(
            file_size, jobs, is_error, report_queue, row_count, start_time
        )
