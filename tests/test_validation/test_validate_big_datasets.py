import logging
import os
import shutil
import time
import uuid
from pathlib import Path

import pytest

from microdata_tools import validate_dataset

logger = logging.getLogger()


def current_milli_time():
    return time.time_ns() // 1_000_000


RESOURCE_DIR = "tests/resources/validation/validate_dataset/big_datasets"

VALID_DATASET_NAMES = [
    "ACCUMULATED_DS",
    # "EVENT_DS",
    # "FIXED_DS",
    # "STATUS_DS",
]


def setup_function():
    logging.basicConfig(level=logging.DEBUG, format="%(message)s", force=True)
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
        if os.path.exists(file_path) and "true" == os.environ.get(
            "MICRODATA_TOOLS_WATCH_MODE"
        ):
            logger.info(f"Skipping generating dataset {dataset_name}")
        else:
            with open(file_path, "w", encoding="utf-8") as f:
                dates = identifier_dates[dataset_name]
                identifier_amount = 28_000_000
                # 2_000_000 if len(dates) == 7 else 14_000_000
                cnt = 0
                for date in dates:
                    for i in range(identifier_amount):
                        cnt += 1
                        if (cnt % 1_000_000) == 0:
                            percent = (cnt * 100) / (
                                identifier_amount * len(dates)
                            )
                            logger.info(f"Wrote {cnt:_} rows. {percent:.1f} %")
                        f.write(f"{i};{i};{date[0]};{date[1]};\n")


def teardown_function():
    for dataset_name in VALID_DATASET_NAMES:
        if os.environ.get("MICRODATA_TOOLS_WATCH_MODE"):
            # print(f'Skipping removing dataset {dataset_name}')
            pass
        else:
            os.remove(f"{RESOURCE_DIR}/{dataset_name}/{dataset_name}.csv")


# @pytest.mark.skipif("not config.getoption('include-big-data')")
# def test_validate_big_dataset():
#     for dataset_name in VALID_DATASET_NAMES:
#         print(f"Validating {dataset_name}")
#         data_errors = validate_dataset(
#             dataset_name,
#             keep_temporary_files=False,
#             input_directory=RESOURCE_DIR,
#         )
#         assert not data_errors


@pytest.mark.focus
def test_validate_big_dataset_perf():
    logging.basicConfig(level=logging.DEBUG, format="%(message)s", force=True)
    working_directory = Path("workdir/" + str(uuid.uuid4()))
    os.makedirs(working_directory)

    try:
        for idx, dataset_name in enumerate(VALID_DATASET_NAMES):
            start_time = current_milli_time()
            if idx != 0:
                logger.info("")
            logger.info(f"Begin {dataset_name} ...")
            data_errors = validate_dataset(
                dataset_name,
                working_directory=working_directory,
                keep_temporary_files=False,
                input_directory=RESOURCE_DIR,
            )
            spent_ms = current_milli_time() - start_time
            assert not data_errors
            logger.info(f"Done {dataset_name}. Spent: {spent_ms:_} ms")
    finally:
        try:
            shutil.rmtree(working_directory)
        except Exception:
            pass


# Sample output original code in validate_dataset:
