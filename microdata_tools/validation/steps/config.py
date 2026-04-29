import os


def tmp_db_file() -> str:
    k = "MICRODATA_TOOLS_TMP_DB_FILE"
    if k in os.environ:
        return os.getenv(k)
    else:
        return "tmp.db"
