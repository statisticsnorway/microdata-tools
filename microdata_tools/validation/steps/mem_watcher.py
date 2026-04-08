import logging
import multiprocessing
import sys

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


def _watch_mem2(
    is_done: multiprocessing.Event, mem_pid_q: multiprocessing.SimpleQueue
) -> tuple[int, int]:
    max_mem = -1
    samples = 0
    processes = {}
    start_time = utils.current_milli_time()
    should_log = utils.log_every_ms(3000)

    old_total_pfaults = 0
    old_total_pageins = 0
    while True:
        done = is_done.wait(0.1)
        if done:
            break

        while not mem_pid_q.empty():
            pid = mem_pid_q.get()
            assert pid not in processes
            proc = psutil.Process(pid)
            processes[pid] = proc

        vms_total = 0
        rss_total = 0
        to_delete = []
        total_pfaults = 0
        total_pageins = 0
        try:
            for pid in processes:
                try:
                    process = processes[pid]
                    # rss, vms, pfaults, pageins = process.memory_info()
                    rss, vms, shared, text, lib, data, dirty, uss, _, _ = (
                        process.memory_full_info()
                    )

                    # total_pfaults += pfaults
                    # total_pageins += pageins
                    vms_total += vms / 1e9
                    rss_total += rss / 1e9
                except psutil.NoSuchProcess:
                    to_delete.append(pid)
            for del_pid in to_delete:
                del processes[del_pid]
        except Exception as e:
            logger.error(f"Error occurred in watch_mem: {str(e)}")
            # logger.error("Error occurred in watch_mem:", e)
            # TODO denne linja er bugga
        spent_ms = utils.current_milli_time() - start_time
        delta_pfaults = total_pfaults - old_total_pfaults
        delta_pageins = total_pageins - old_total_pageins
        old_total_pfaults = total_pfaults
        old_total_pageins = total_pageins
        if should_log():
            logger.info(
                f"Δ pfaults: {delta_pfaults:_} "
                + f"Δ pageins: {delta_pageins:_} "
                + f"Resident mem: {rss_total:.1f} GB, "
                + f"Virtual mem: {vms_total:.1f} GB, "
                + f"uptime: {utils.ms_to_eta(spent_ms)}"
            )

        samples += 1
        if vms_total > max_mem:
            max_mem = vms_total
    return samples, max_mem


def watch_mem() -> None:
    global _is_done
    global _mem_pid_q
    _init_logging()
    try:
        return _watch_mem2(_is_done, _mem_pid_q)
    except Exception as e:
        logger.error("Error occurred in watch_mem:", e)
        return -1, -1
