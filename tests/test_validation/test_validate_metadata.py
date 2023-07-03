import os
import json
import logging
from pathlib import Path

import pytest

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
INVALID_SENSITIVITY_METADATA = "INVALID_SENSITIVITY_DATASET"
EMPTY_CODELIST_METADATA = "EMPTY_CODELIST_DATASET"
EXTRA_FIELDS_METADATA = "EXTRA_FIELDS_DATASET"
EXTRA_FIELDS_UNIT_MEASURE_METADATA = "EXTRA_FIELDS_UNIT_MEASURE_DATASET"
INVALID_UNIT_TYPE_DATASET = "INVALID_UNIT_TYPE_DATASET"


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


def test_validate_invalid_unit_type_dataset():
    data_errors = validate_metadata(
        INVALID_UNIT_TYPE_DATASET,
        working_directory=WORKING_DIR,
        keep_temporary_files=True,
        input_directory=INPUT_DIR,
    )
    assert len(data_errors) == 1
    assert "value is not a valid enumeration member" in data_errors[0]


def test_invalid_sensitivity():
    data_errors = validate_metadata(INVALID_SENSITIVITY_METADATA, INPUT_DIR)
    assert len(data_errors) == 3
    assert "sensitivityLevel" in data_errors[0]
    assert "value is not a valid enumeration member" in data_errors[0]


def test_validate_invalid_metadata():
    data_errors = validate_metadata(
        MISSING_IDENTIFIER_METADATA, input_directory=INPUT_DIR
    )
    assert "identifierVariables" in data_errors[0]
    assert "field required" in data_errors[0]


def test_validate_empty_codelist():
    data_errors = validate_metadata(
        EMPTY_CODELIST_METADATA, input_directory=INPUT_DIR
    )
    assert "codeList" in data_errors[0]
    assert "ensure this value has at least 1 items" in data_errors[0]


def test_invalidate_extra_fields():
    data_errors = validate_metadata(
        EXTRA_FIELDS_METADATA, input_directory=INPUT_DIR
    )
    assert len(data_errors) == 4
    for data_error in data_errors:
        assert "extra fields not permitted" in data_error


def test_invalidate_extra_fields_unit_type_measure():
    data_errors = validate_metadata(
        EXTRA_FIELDS_UNIT_MEASURE_METADATA, input_directory=INPUT_DIR
    )
    assert len(data_errors) == 1
    assert (
        "Can not set a dataType in a measure variable together with a unitType"
    ) in data_errors[0]


def test_validate_metadata_does_not_exist():
    with pytest.raises(FileNotFoundError):
        validate_metadata(NO_SUCH_METADATA, INPUT_DIR)


def get_working_directory_files() -> list:
    return os.listdir(WORKING_DIR)


def teardown_function():
    for file in get_working_directory_files():
        if file != ".gitkeep":
            os.remove(f"{WORKING_DIR}/{file}")
