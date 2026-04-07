import time
from functools import partial
from typing import Callable


def current_milli_time() -> int:
    return time.time_ns() // 1_000_000


def log_time() -> int:
    return time.time_ns() // 1_000_000 // 1000


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


def _log_every_ms(ms: int, state: dict[str, int]) -> bool:
    last_log = state["last_log"]
    lst_log = current_milli_time() // ms
    if last_log == lst_log:
        return False
    else:
        state["last_log"] = lst_log
        return True


def log_every_ms(ms: int) -> Callable[[], bool]:
    return partial(_log_every_ms, ms, {"last_log": -1})
