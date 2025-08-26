from pathlib import Path

import pytest

from microdata_tools.validation.components import (
    temporal_attributes,
    unit_type_variables,
)
from microdata_tools.validation.exceptions import ValidationError
from microdata_tools.validation.steps import metadata_reader

INPUT_DIR = Path("tests/resources/validation/steps/metadata_reader")
MEASURE_DATATYPE = "STRING"
VALID_CODE_LIST = [
    {
        "code": "a",
        "description": "a",
        "validFrom": "2020-01-01",
        "validUntil": "2020-12-31",
    },
    {
        "code": "b",
        "description": "b",
        "validFrom": "2020-01-01",
        "validUntil": "2020-12-31",
    },
    {"code": "a", "description": "a", "validFrom": "2021-01-01"},
]
OVERLAP_CODE_LIST = [code for code in VALID_CODE_LIST] + [
    {
        "code": "a",
        "description": "a",
        "validFrom": "2020-09-01",
        "validUntil": "2020-12-31",
    },
]


def test_read_valid_dataset():
    DATASET_NAME = "VALID"
    METADATA_PATH = INPUT_DIR / f"{DATASET_NAME}.json"

    metadata = metadata_reader.run_reader(DATASET_NAME, METADATA_PATH)

    assert metadata["shortName"] == DATASET_NAME
    assert len(metadata["measureVariables"]) == 1
    assert metadata["measureVariables"][0]["shortName"] == DATASET_NAME
    assert len(metadata["identifierVariables"]) == 1
    assert metadata["identifierVariables"][0] == unit_type_variables.get(
        "PERSON"
    )
    assert len(metadata["attributeVariables"]) == 2
    assert (
        temporal_attributes.generate_start_time_attribute("ACCUMULATED")
        in metadata["attributeVariables"]
    )
    assert (
        temporal_attributes.generate_stop_time_attribute("ACCUMULATED")
        in metadata["attributeVariables"]
    )


def test_read_unit_measure():
    DATASET_NAME = "UNIT_MEASURE"
    METADATA_PATH = INPUT_DIR / f"{DATASET_NAME}.json"

    metadata = metadata_reader.run_reader(DATASET_NAME, METADATA_PATH)

    assert metadata["shortName"] == DATASET_NAME
    assert len(metadata["measureVariables"]) == 1

    unit_measure_definition = unit_type_variables.get("PERSON")
    unit_measure_definition["shortName"] = DATASET_NAME
    unit_measure_definition["name"] = [
        {"languageCode": "no", "value": "Mor FNR"}
    ]
    unit_measure_definition["description"] = [
        {"languageCode": "no", "value": "Fødselsnummer til Mor"}
    ]
    assert metadata["measureVariables"][0] == unit_measure_definition
    assert len(metadata["identifierVariables"]) == 1
    assert metadata["identifierVariables"][0] == unit_type_variables.get(
        "PERSON"
    )
    assert len(metadata["attributeVariables"]) == 2
    assert (
        temporal_attributes.generate_start_time_attribute("ACCUMULATED")
        in metadata["attributeVariables"]
    )
    assert (
        temporal_attributes.generate_stop_time_attribute("ACCUMULATED")
        in metadata["attributeVariables"]
    )


def test_read_empty_codelist():
    DATASET_NAME = "EMPTY_CODELIST"
    METADATA_PATH = INPUT_DIR / f"{DATASET_NAME}.json"
    with pytest.raises(ValidationError) as e:
        metadata_reader.run_reader(DATASET_NAME, METADATA_PATH)
    assert "Errors found while validating metadata file" in str(e)
    assert e.value.errors == [
        "measureVariables->valueDomain->codeList: "
        "List should have at least 1 item after validation, not 0"
    ]


def test_read_extra_fields():
    DATASET_NAME = "EXTRA_FIELDS"
    METADATA_PATH = INPUT_DIR / f"{DATASET_NAME}.json"
    with pytest.raises(ValidationError) as e:
        metadata_reader.run_reader(DATASET_NAME, METADATA_PATH)
    assert "Errors found while validating metadata file" in str(e)
    assert e.value.errors == [
        "dataRevision->newTypeOfField: Extra inputs are not permitted",
        "identifierVariables->invalidField: Extra inputs are not permitted",
        "measureVariables->valueDomain->codeList->validTo: Extra inputs are not permitted",
        "measureVariables->valueDomain->sentinelAndMissingValues->validFrom: Extra "
        "inputs are not permitted",
    ]


def test_read_extra_fields_unit_measure():
    DATASET_NAME = "EXTRA_FIELDS_UNIT_MEASURE"
    METADATA_PATH = INPUT_DIR / f"{DATASET_NAME}.json"
    with pytest.raises(ValidationError) as e:
        metadata_reader.run_reader(DATASET_NAME, METADATA_PATH)
    assert "Errors found while validating metadata file" in str(e)
    assert e.value.errors == [
        "measureVariables: Value error, Can not set a dataType in a measure variable "
        "together with a unitType"
    ]


def test_read_invalid_sensitivity():
    DATASET_NAME = "INVALID_SENSITIVITY"
    METADATA_PATH = INPUT_DIR / f"{DATASET_NAME}.json"
    with pytest.raises(ValidationError) as e:
        metadata_reader.run_reader(DATASET_NAME, METADATA_PATH)
    assert "Errors found while validating metadata file" in str(e)
    assert e.value.errors == [
        (
            "sensitivityLevel: Input should be 'PERSON_GENERAL', 'PERSON_SPECIAL',"
            " 'PUBLIC' or 'NONPUBLIC'"
        )
    ]


def test_read_invalid_unit_type():
    DATASET_NAME = "INVALID_UNIT_TYPE"
    METADATA_PATH = INPUT_DIR / f"{DATASET_NAME}.json"
    with pytest.raises(ValidationError) as e:
        metadata_reader.run_reader(DATASET_NAME, METADATA_PATH)
    assert "Errors found while validating metadata file" in str(e)
    assert (
        "identifierVariables->unitType: Input should be 'JOBB', 'KJORETOY',"
        " 'FAMILIE', 'FORETAK', 'HUSHOLDNING', 'KOMMUNE',"
    ) in e.value.errors[0]


def test_read_missing_identifier():
    DATASET_NAME = "MISSING_IDENTIFIER"
    METADATA_PATH = INPUT_DIR / f"{DATASET_NAME}.json"
    with pytest.raises(ValidationError) as e:
        metadata_reader.run_reader(DATASET_NAME, METADATA_PATH)
    assert "Errors found while validating metadata file" in str(e)
    assert e.value.errors == ["identifierVariables: Field required"]


def test_code_list_validation():
    code_list_errors = metadata_reader._validate_code_list(
        VALID_CODE_LIST, MEASURE_DATATYPE
    )
    assert not code_list_errors

    code_list_errors = metadata_reader._validate_code_list(
        OVERLAP_CODE_LIST, MEASURE_DATATYPE
    )
    assert code_list_errors == ["Duplicate codes for same time period: ['a']"]


def test_mismatch_between_specified_datatype_and_datatype_within_codelist():
    DATASET_NAME = "MISMATCHING_DATATYPE_WITH_CODELIST"
    METADATA_PATH = INPUT_DIR / f"{DATASET_NAME}.json"
    with pytest.raises(ValidationError) as e:
        metadata_reader.run_reader(DATASET_NAME, METADATA_PATH)
    assert e.value.errors == [
        "Specified data type for measure (LONG) does not match the data type within the codelist (STRING). Codes with mismatching data type are: ['1', '2', '3', '4', '5', '...']"
    ]


def test_mismatch_between_specified_datatype_and_datatype_within_sentinel_list():
    DATASET_NAME = "MISMATCHING_DATATYPE_WITH_SENTINEL_LIST"
    METADATA_PATH = INPUT_DIR / f"{DATASET_NAME}.json"
    with pytest.raises(ValidationError) as e:
        metadata_reader.run_reader(DATASET_NAME, METADATA_PATH)
    assert e.value.errors == [
        "Specified data type for measure (STRING) does not match the data type within the sentinel- and missing values list (LONG). Codes with mismatching data type are: [0]"
    ]
