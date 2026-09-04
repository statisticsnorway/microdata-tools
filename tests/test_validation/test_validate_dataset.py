import json
import logging
import os
from datetime import datetime

from pyarrow import dataset

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
    # A FIXED dataset in the depercated format, with a stop date on
    # every row. Still valid, see test_validate_legacy_fixed_dataset.
    "LEGACY_FIXED",
]
NO_SUCH_DATASET_NAME = "NO_SUCH_DATASET"
WRONG_DELIMITER_DATASET_NAME = "WRONG_DELIMITER_DATASET"


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
        if expected_metadata["temporalityType"] == "FIXED":
            # A FIXED dataset is valid up until the time of validation,
            # so its temporalCoverageLatest is not a fixed date.
            expected_metadata["dataRevision"]["temporalCoverageLatest"] = (
                datetime.now().strftime("%Y-%m-%d")
            )
        assert actual_metadata == expected_metadata


def test_validate_legacy_fixed_dataset(caplog):
    """
    Before the stop column was deprecated for FIXED datasets, the date
    the data was valid until was repeated on every row. Such datasets
    are still valid: the dates are ignored, and the producer is warned.
    """
    DATASET_NAME = "LEGACY_FIXED"
    with caplog.at_level(logging.WARNING):
        data_errors = validate_dataset(
            DATASET_NAME,
            working_directory=WORKING_DIR,
            keep_temporary_files=True,
            input_directory=INPUT_DIR,
        )
    assert not data_errors
    assert "DEPRECATED" in caplog.text
    assert "#4 column" in caplog.text

    with open(f"{WORKING_DIR}/{DATASET_NAME}.json", "r", encoding="utf-8") as f:
        metadata = json.load(f)
    data_revision = metadata["dataRevision"]
    # The stop dates in the data file are not used as temporal coverage
    assert data_revision["temporalCoverageStart"] == "1900-01-01"
    assert data_revision["temporalCoverageLatest"] == datetime.now().strftime(
        "%Y-%m-%d"
    )

    # The deprecated stop dates are dropped from the validated data
    table = dataset.dataset(f"{WORKING_DIR}/{DATASET_NAME}.parquet").to_table()
    assert table.column("stop_epoch_days").null_count == table.num_rows
    assert table.column("start_epoch_days").null_count == table.num_rows


def test_invalid_dataset_name():
    data_errors = validate_dataset(
        "1_INVALID_DATASET_NAME",
        input_directory=INPUT_DIR,
    )
    assert data_errors == [
        '"1_INVALID_DATASET_NAME" contains invalid characters. '
        'Please use only uppercase A-Z, numbers 0-9 or "_"'
    ]


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
            dir
            for dir in os.listdir()
            if os.path.isdir(dir)
            and not (dir.startswith(".") or dir.endswith(".egg-info"))
        ]
        assert not data_errors
        for file in temp_files:
            assert file in [
                "tmp",
                "mise-tasks",
                "scripts",
                "tests",
                "docs",
                "microdata_tools",
            ]


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
    data_errors = validate_dataset(
        NO_SUCH_DATASET_NAME,
        working_directory=WORKING_DIR,
        input_directory=INPUT_DIR,
    )
    assert len(data_errors) == 1
    assert "not found" in data_errors[0]


def get_working_directory_files() -> list:
    return os.listdir(WORKING_DIR)


def teardown_function():
    for file in get_working_directory_files():
        if file != ".gitkeep":
            os.remove(f"{WORKING_DIR}/{file}")
