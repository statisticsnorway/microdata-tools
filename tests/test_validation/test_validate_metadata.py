import json
import logging
import os
from pathlib import Path

from microdata_tools import validate_metadata

logger = logging.getLogger()
logger.setLevel(logging.INFO)

RESOURCE_DIR = "tests/resources/validation/validate_metadata"
INPUT_DIR = f"{RESOURCE_DIR}/input_directory"
WORKING_DIR = f"{RESOURCE_DIR}/working_directory"
EXPECTED_DIR = f"{RESOURCE_DIR}/expected"

VALID_METADATA = ["SYNT_BEFOLKNING_SIVSTAND", "SYNT_PERSON_INNTEKT"]

NO_SUCH_METADATA = "NO_SUCH_METADATA"
MISSING_IDENTIFIER_METADATA = "MISSING_IDENTIFIER_DATASET"
EMPTY_STRING_METADATA = "EMPTY_STRING_METADATA"


def test_validate_valid_metadata():
    for metadata in VALID_METADATA:
        data_errors = validate_metadata(
            metadata,
            input_directory=INPUT_DIR,
            working_directory=WORKING_DIR,
            keep_temporary_files=True,
        )
        with open(
            Path(WORKING_DIR) / f"{metadata}.json", "r", encoding="utf-8"
        ) as f:
            actual_metadata = json.load(f)
        with open(
            Path(EXPECTED_DIR) / f"{metadata}.json",
            "r",
            encoding="utf-8",
        ) as f:
            expected_metadata = json.load(f)
        assert actual_metadata == expected_metadata
        assert not data_errors


def test_validate_invalid_metadata():
    data_errors = validate_metadata(
        MISSING_IDENTIFIER_METADATA, input_directory=INPUT_DIR
    )
    assert "identifierVariables" in data_errors[0]
    assert "Field required" in data_errors[0]


def test_invalid_dataset_name():
    data_errors = validate_metadata(
        "1_INVALID_DATASET_NAME",
        input_directory=INPUT_DIR,
    )
    assert data_errors == [
        '"1_INVALID_DATASET_NAME" contains invalid characters. '
        'Please use only uppercase A-Z, numbers 0-9 or "_"'
    ]


def test_validate_metadata_does_not_exist():
    data_errors = validate_metadata(NO_SUCH_METADATA, INPUT_DIR)
    assert len(data_errors) == 1
    assert "not found" in data_errors[0]


def test_validate_metadata_empty_string():
    data_errors = validate_metadata(EMPTY_STRING_METADATA, INPUT_DIR)
    assert len(data_errors) == 1
    assert (
        "measureVariables->name->value: String should have at least 1 character"
        in data_errors[0]
    )


def get_working_directory_files() -> list:
    return os.listdir(WORKING_DIR)


def teardown_function():
    for file in get_working_directory_files():
        if file != ".gitkeep":
            os.remove(f"{WORKING_DIR}/{file}")
