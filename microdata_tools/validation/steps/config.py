import os


def tmp_db_file() -> str:
    return work_dir() + "/tmp.db"


def work_dir() -> str:
    k = "MICRODATA_TOOLS_WORK_DIR"
    assert k in os.environ
    return os.getenv(k)
    if k in os.environ:
        return os.getenv(k)
    else:
        return "tmp.db"
