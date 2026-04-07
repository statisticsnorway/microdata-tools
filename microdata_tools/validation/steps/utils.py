import time


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
