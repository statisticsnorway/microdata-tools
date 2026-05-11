import logging
import os
import shutil
import uuid
from pathlib import Path

import pytest

from microdata_tools import validate_dataset
from microdata_tools.validation._utils import (
    current_milli_time,
    log_time,
    ms_to_eta,
)

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


def setup_function():
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
        if os.path.exists(file_path) and row_count == _get_row_count(
            Path(file_path)
        ):
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
    try:
        shutil.rmtree("tmp")
    except Exception:
        pass


@pytest.mark.skipif("not config.getoption('include-big-data')")
def test_validate_big_dataset():
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
