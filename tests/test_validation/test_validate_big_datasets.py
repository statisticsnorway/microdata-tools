import logging
import os
import shutil
import uuid
from pathlib import Path

import pytest

from microdata_tools import validate_dataset

logger = logging.getLogger()

RESOURCE_DIR = "tests/resources/validation/validate_dataset/big_datasets"

VALID_DATASET_NAMES = [
    "ACCUMULATED_DS",
    "EVENT_DS",
    "FIXED_DS",
    "STATUS_DS",
]


def _get_row_count(csv_path: Path) -> int:
    if not os.path.exists(csv_path):
        return -1

    rowcount_path = Path(str(csv_path) + ".rowcount")
    if os.path.exists(rowcount_path) and os.path.getmtime(
        rowcount_path
    ) >= os.path.getmtime(csv_path):
        with open(rowcount_path) as f:
            return int(f.read().strip())
    return -1


def setup_fn(row_count):
    # The dates are intentionally shuffled to provoke errors
    identifier_dates = {
        "ACCUMULATED_DS": [
            ("2020-01-01", "2020-12-31"),
            ("2015-01-01", "2015-12-31"),
            ("2012-01-01", "2012-12-31"),
            ("2011-01-01", "2011-12-31"),
            ("2013-01-01", "2013-12-31"),
            ("2010-01-01", "2010-12-31"),
            ("2007-01-01", "2007-12-31"),
        ],
        "EVENT_DS": [
            ("2020-01-01", "2020-06-01"),
            ("2015-01-01", "2015-06-01"),
            ("2012-01-01", "2014-12-31"),
            ("2011-01-01", "2011-12-31"),
            ("2020-06-02", ""),
            ("2015-06-02", "2019-12-31"),
            ("2010-01-01", "2010-12-31"),
        ],
        "STATUS_DS": [
            ("2020-01-01", "2020-01-01"),
            ("2015-01-01", "2015-01-01"),
            ("2012-01-01", "2012-01-01"),
            ("2011-01-01", "2011-01-01"),
            ("2013-01-01", "2013-01-01"),
            ("2010-01-01", "2010-01-01"),
            ("2007-01-01", "2007-01-01"),
        ],
        "FIXED_DS": [("", "2020-01-01")],
    }
    for dataset_name in VALID_DATASET_NAMES:
        file_path = f"{RESOURCE_DIR}/{dataset_name}/{dataset_name}.csv"
        if os.path.exists(file_path) and row_count == _get_row_count(
            Path(file_path)
        ):
            continue
        with open(file_path, "w", encoding="utf-8") as f:
            dates = identifier_dates[dataset_name]
            # len(dates) * identifier_amount = row_count
            identifier_amount = 1 + (row_count // len(dates))
            cnt = 0
            for date in dates:
                for i in range(identifier_amount):
                    cnt += 1
                    f.write(f"{i};{i};{date[0]};{date[1]};\n")
                    if cnt == row_count:
                        break
                if cnt == row_count:
                    break
        with open(file_path + ".rowcount", "w", encoding="utf-8") as f:
            f.write(str(row_count))


@pytest.fixture(autouse=True)
def setup(pytestconfig):
    keep_big_data_csv_files = pytestconfig.getoption(
        "--keep-big-data-csv-files"
    )
    big_data_row_count = pytestconfig.getoption("--big-data-row-count")
    row_count = int(big_data_row_count.strip().replace("_", ""))
    assert row_count >= 1
    try:
        setup_fn(row_count)
        yield
    finally:
        teardown(keep_big_data_csv_files)


def teardown(keep_big_data_csv_files: bool):
    for dataset_name in VALID_DATASET_NAMES:
        if not keep_big_data_csv_files:
            os.remove(f"{RESOURCE_DIR}/{dataset_name}/{dataset_name}.csv")
    if os.path.exists("tmp"):
        shutil.rmtree("tmp")


@pytest.mark.skipif("not config.getoption('include-big-data')")
def test_validate_big_dataset(pytestconfig):
    generated_working_directory = "tmp/" + str(uuid.uuid4())
    os.makedirs(generated_working_directory, exist_ok=True)
    for dataset_name in VALID_DATASET_NAMES:
        print(f"Validating {dataset_name}")
        data_errors = validate_dataset(
            dataset_name,
            keep_temporary_files=False,
            input_directory=RESOURCE_DIR,
            working_directory=generated_working_directory,
        )
        assert not data_errors
