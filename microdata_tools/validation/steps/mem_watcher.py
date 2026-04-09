import logging
import multiprocessing
import sys
import traceback

import psutil

from microdata_tools.validation.steps import utils

logger = logging.getLogger()

_is_done = None
_mem_pid_q: multiprocessing.SimpleQueue


def _init_logging() -> None:
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s.%(msecs)03d %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout,
        force=True,
    )


def init_mem_watcher(
    is_done: multiprocessing.Event, mem_pid_q: multiprocessing.SimpleQueue
) -> None:
    _init_logging()
    global _is_done
    global _mem_pid_q
    _is_done = is_done
    _mem_pid_q = mem_pid_q


def _watch_mem(
    is_done: multiprocessing.Event, mem_pid_q: multiprocessing.SimpleQueue
) -> tuple[int, int]:
    max_mem = -1
    samples = 0
    processes = {}
    start_time = utils.current_milli_time()
    should_log = utils.log_every_ms(3000)

    while True:
        done = is_done.wait(0.1)
        if done:
            break

        while not mem_pid_q.empty():
            pid = mem_pid_q.get()
            assert pid not in processes
            proc = psutil.Process(pid)
            processes[pid] = proc

        rss_total = 0
        to_delete = []
        try:
            for pid in processes:
                try:
                    process = processes[pid]
                    rss = process.memory_info().rss
                    rss_total += rss
                except psutil.NoSuchProcess:
                    to_delete.append(pid)
            for del_pid in to_delete:
                del processes[del_pid]
        except Exception:
            logger.error(
                "Error occurred in watch_mem: "
                + f"{str(traceback.format_exc())}"
            )
            # logger.error("Error occurred in watch_mem:", e)
            # TODO denne linja er bugga
        spent_ms = utils.current_milli_time() - start_time
        if should_log():
            logger.info(
                f"rss: {rss_total / 1e6:.0f} MiB, "
                + f"uptime: {utils.ms_to_eta(spent_ms)}"
            )

        samples += 1
        if rss_total > max_mem:
            max_mem = rss_total
    return samples, max_mem


def watch_mem() -> tuple[int, int]:
    global _is_done
    global _mem_pid_q
    _init_logging()
    try:
        return _watch_mem(_is_done, _mem_pid_q)
    except Exception as e:
        logger.error("Error occurred in watch_mem:", e)
        return -1, -1
