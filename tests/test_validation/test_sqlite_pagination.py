import logging
import os
import sqlite3

import pytest

logger = logging.getLogger()


def init_logging():
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s.%(msecs)03d %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        force=True,
    )


def teardown_function():
    if os.path.exists("tst_pagination.db"):
        os.remove("tst_pagination.db")


def test_pagination(row_count, num_cores):
    if os.path.exists("tst_pagination.db"):
        os.remove("tst_pagination.db")

    with sqlite3.connect("tst_pagination.db", autocommit=False) as conn:
        conn.execute("""CREATE TABLE dataset(unit_id VARCHAR)""")
        conn.commit()
        for unit_id in range(row_count):
            conn.execute(
                "INSERT INTO dataset (unit_id) VALUES (?)", (unit_id + 1,)
            )
        conn.commit()

        assert (
            row_count
            == conn.execute("SELECT count(*) FROM dataset").fetchone()[0]
        )
        chunk_size = (row_count // num_cores) + 1
        assert chunk_size >= 1

        s = 0
        seen_unit_ids = []
        iterations = 0
        while s <= row_count:
            res = conn.execute(
                "SELECT unit_id FROM dataset order by unit_id LIMIT ? OFFSET ?",
                (chunk_size, s),
            ).fetchall()
            iterations += 1
            res_ids = [x[0] for x in res]
            for unit_id in res_ids:
                if unit_id in seen_unit_ids:
                    raise RuntimeError(f"Unit id {unit_id} already seen")
                else:
                    seen_unit_ids.append(unit_id)
            s += chunk_size
        assert len(seen_unit_ids) == row_count
        assert iterations <= num_cores


@pytest.mark.focus
def test_sqlite_pagination():
    init_logging()

    test_pagination(1, 1)
    test_pagination(5, 8)
    test_pagination(4, 8)
    test_pagination(1000, 8)
    test_pagination(999, 8)
    test_pagination(333, 3)

    test_pagination(999, 2)
    test_pagination(1000, 2)

    test_pagination(1000, 1)
    test_pagination(1000, 3)
    test_pagination(999, 3)
    test_pagination(777, 3)
