import logging
import os
import shutil
import uuid
from pathlib import Path

import pytest

from microdata_tools import validate_dataset
from microdata_tools.validation.steps import config, reader_utils
from microdata_tools.validation.steps.utils import (
    current_milli_time,
    log_time,
    ms_to_eta,
)
from tests import log_setup

logger = logging.getLogger()

if "MICRODATA_TOOLS_TEST_DISK" in os.environ:
    _PREFIX = os.getenv("MICRODATA_TOOLS_TEST_DISK")
else:
    _PREFIX = "."

RESOURCE_DIR = (
    f"{_PREFIX}/tests/resources/validation/validate_dataset/big_datasets"
)

VALID_DATASET_NAMES = [
    "ACCUMULATED_DS",
    # "EVENT_DS",
    # "FIXED_DS",
    # "STATUS_DS",
]


def setup_function():
    log_setup.init_logging()
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
        row_count = os.environ.get("MICRODATA_TOOLS_ROW_COUNT", "10_000_000")
        row_count = int(row_count.strip().replace("_", ""))
        file_path = f"{RESOURCE_DIR}/{dataset_name}/{dataset_name}.csv"
        if os.path.exists(
            file_path
        ) and row_count == reader_utils.get_row_count(Path(file_path)):
            logger.info(f"File path is: {file_path}")
            logger.info(f"Skipping generating dataset {dataset_name}")
        else:
            last_log = log_time()
            start_ms = current_milli_time()
            with open(file_path, "w", encoding="utf-8") as f:
                logger.info(f"Generating dataset {dataset_name}")
                logger.info(f"File path: {file_path}")
                dates = identifier_dates[dataset_name]

                # len(dates) * identifier_amount = row_count
                identifier_amount = 1 + (row_count // len(dates))
                cnt = 0
                logger.info(f"Identifier amount: {identifier_amount:_}")
                logger.info(f"Number of rows: {row_count:_}")
                for date in dates:
                    for i in range(identifier_amount):
                        cnt += 1
                        lst_log = log_time()
                        if lst_log == last_log and cnt != row_count:
                            pass
                        else:
                            last_log = lst_log
                            percent = (cnt * 100) / row_count
                            spent_ms_so_far = current_milli_time() - start_ms
                            ms_per_row = spent_ms_so_far / cnt
                            remaining_ms = ms_per_row * (row_count - cnt)
                            cnt_str = f"{cnt:_}".rjust(len(f"{row_count:_}"))
                            percent_str = f"{percent:.1f}".rjust(len("100.0"))
                            eta = f"{ms_to_eta(int(remaining_ms))}"
                            logger.info(
                                f"Generated {cnt_str} rows. "
                                + f"{percent_str} % done. "
                                + f"ETA: {eta}"
                            )
                        f.write(f"{i};{i};{date[0]};{date[1]};\n")
                        if cnt == row_count:
                            break
                    if cnt == row_count:
                        break
            with open(file_path + ".rowcount", "w", encoding="utf-8") as f:
                f.write(str(row_count))


def teardown_function():
    for dataset_name in VALID_DATASET_NAMES:
        if "false" == os.environ.get("MICRODATA_TOOLS_DELETE_FILES"):
            # print(f'Skipping removing dataset {dataset_name}')
            pass
        else:
            os.remove(f"{RESOURCE_DIR}/{dataset_name}/{dataset_name}.csv")


@pytest.mark.perf_init
def test_validate_init():
    log_setup.init_logging()


@pytest.mark.perf_new
def test_validate_big_dataset_perf():
    log_setup.init_logging()
    logger.info(f"self pid is {os.getpid()}")
    working_directory = Path(config.work_dir() + "/" + str(uuid.uuid4()))
    os.makedirs(working_directory)

    logger.info(f"Main worker pid is: {str(os.getpid())}")
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
