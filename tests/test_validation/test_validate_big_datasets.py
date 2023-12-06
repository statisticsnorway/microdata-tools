import os

import pytest

from microdata_tools import validate_dataset


RESOURCE_DIR = "tests/resources/validation/validate_dataset/big_datasets"

VALID_DATASET_NAMES = [
    "ACCUMULATED_DS",
    "EVENT_DS",
    "FIXED_DS",
    "STATUS_DS",
]


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
        file_path = f"{RESOURCE_DIR}/{dataset_name}/{dataset_name}.csv"
        with open(file_path, "w", encoding="utf-8") as f:
            dates = identifier_dates[dataset_name]
            identifier_amount = 2_000_000 if len(dates) == 7 else 14_000_000
            for date in dates:
                for i in range(identifier_amount):
                    f.write(f"{i};{i};{date[0]};{date[1]};\n")


def teardown_function():
    for dataset_name in VALID_DATASET_NAMES:
        os.remove(f"{RESOURCE_DIR}/{dataset_name}/{dataset_name}.csv")


@pytest.mark.skipif("not config.getoption('include-big-data')")
def test_validate_big_dataset():
    for dataset_name in VALID_DATASET_NAMES:
        print(f"Validating {dataset_name}")
        data_errors = validate_dataset(
            dataset_name,
            keep_temporary_files=False,
            input_directory=RESOURCE_DIR,
        )
        assert not data_errors
