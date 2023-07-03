import json
import os

import pytest

from microdata_tools import validate_dataset


RESOURCE_DIR = "tests/resources/validation/validate_dataset"
INPUT_DIR = f"{RESOURCE_DIR}/input_directory"
WORKING_DIR = f"{RESOURCE_DIR}/working_directory"
EXPECTED_DIR = f"{RESOURCE_DIR}/expected"

VALID_DATASET_NAMES = [
    "SYNT_BEFOLKNING_SIVSTAND",
    "SYNT_PERSON_INNTEKT",
    "SYNT_PERSON_MOR",
    "SYNT_UTDANNING",
]
NO_SUCH_DATASET_NAME = "NO_SUCH_DATASET"
WRONG_DELIMITER_DATASET_NAME = "WRONG_DELIMITER_DATASET"
INVALID_DATES_DATASET_NAME = "INVALID_DATES_DATASET"
INVALID_DATE_RANGES_DATASET_NAME = "INVALID_DATE_RANGES_DATASET"


def test_validate_valid_dataset():
    for dataset_name in VALID_DATASET_NAMES:
        data_errors = validate_dataset(
            dataset_name,
            working_directory=WORKING_DIR,
            keep_temporary_files=True,
            input_directory=INPUT_DIR,
        )
        actual_files = get_working_directory_files()
        expected_files = [
            f"{dataset_name}.json",
            f"{dataset_name}.parquet",
        ]
        assert not data_errors
        for file in expected_files:
            assert file in actual_files
        with open(
            f"{WORKING_DIR}/{dataset_name}.json", "r", encoding="utf-8"
        ) as f:
            actual_metadata = json.load(f)
        with open(
            f"{EXPECTED_DIR}/{dataset_name}.json", "r", encoding="utf-8"
        ) as f:
            expected_metadata = json.load(f)
        assert actual_metadata == expected_metadata


def test_validate_valid_dataset_wrong_delimiter():
    data_errors = validate_dataset(
        WRONG_DELIMITER_DATASET_NAME,
        working_directory=WORKING_DIR,
        keep_temporary_files=True,
        input_directory=INPUT_DIR,
    )
    assert data_errors == ['Invalid field separator ",". Use ";".']


def test_validate_valid_dataset_delete_temporary_files():
    for valid_dataset_name in VALID_DATASET_NAMES:
        data_errors = validate_dataset(
            valid_dataset_name,
            working_directory=WORKING_DIR,
            input_directory=INPUT_DIR,
        )
        temp_files = get_working_directory_files()
        assert not data_errors
        assert temp_files == [".gitkeep"]


def test_validate_valid_dataset_delete_generated_dir():
    for valid_dataset_name in VALID_DATASET_NAMES:
        data_errors = validate_dataset(
            valid_dataset_name, input_directory=INPUT_DIR
        )
        temp_files = [
            dir for dir in os.listdir() if os.path.isdir(dir) and dir[0] != "."
        ]
        assert not data_errors
        for file in temp_files:
            assert file in ["tests", "docs", "microdata_validator"]


def test_validate_valid_dataset_delete_working_files():
    for valid_dataset_name in VALID_DATASET_NAMES:
        data_errors = validate_dataset(
            valid_dataset_name,
            working_directory=WORKING_DIR,
            input_directory=INPUT_DIR,
        )
        actual_files = get_working_directory_files()
        assert not data_errors
        assert actual_files == [".gitkeep"]


def test_dataset_does_not_exist():
    with pytest.raises(FileNotFoundError):
        validate_dataset(
            NO_SUCH_DATASET_NAME,
            working_directory=WORKING_DIR,
            input_directory=INPUT_DIR,
        )


def test_validate_invalid_dates():
    data_errors = validate_dataset(
        INVALID_DATES_DATASET_NAME,
        working_directory=WORKING_DIR,
        input_directory=INPUT_DIR,
    )
    assert data_errors == [
        'row 4: STOP date not valid - "1927-13-01"',
        'row 15: STOP date not valid - "1940-02-31"',
    ]


def test_invalid_date_ranges():
    data_errors = validate_dataset(
        INVALID_DATE_RANGES_DATASET_NAME,
        working_directory=WORKING_DIR,
        input_directory=INPUT_DIR,
    )
    assert data_errors == [
        (
            "row 4: Previous STOP-date greater than STOP-date "
            "- 1926-02-02 > 1926-02-01"
        ),
        (
            "row 13: Previous STOP-date greater than STOP-date"
            " - 1940-01-31 > 1939-02-01"
        ),
    ]


def get_working_directory_files() -> list:
    return os.listdir(WORKING_DIR)


def teardown_function():
    for file in get_working_directory_files():
        if file != ".gitkeep":
            os.remove(f"{WORKING_DIR}/{file}")
