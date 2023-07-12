from pathlib import Path

import pytest
from microdata_tools.validation.exceptions import ValidationError


from microdata_tools.validation.steps import metadata_reader
from microdata_tools.validation.components import (
    temporal_attributes,
    unit_type_variables,
)

INPUT_DIR = Path("tests/resources/validation/steps/metadata_reader")


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
        "measureVariables->valueDomain->codeList: ensure this value has at "
        "least 1 items"
    ]


def test_read_extra_fields():
    DATASET_NAME = "EXTRA_FIELDS"
    METADATA_PATH = INPUT_DIR / f"{DATASET_NAME}.json"
    with pytest.raises(ValidationError) as e:
        metadata_reader.run_reader(DATASET_NAME, METADATA_PATH)
    assert "Errors found while validating metadata file" in str(e)
    assert e.value.errors == [
        "dataRevision->newTypeOfField: extra fields not permitted",
        "identifierVariables->invalidField: extra fields not permitted",
        "measureVariables->valueDomain->codeList->validTo: extra fields not permitted",
        "measureVariables->valueDomain->sentinelAndMissingValues->validFrom: extra "
        "fields not permitted",
    ]


def test_read_extra_fields_unit_measure():
    DATASET_NAME = "EXTRA_FIELDS_UNIT_MEASURE"
    METADATA_PATH = INPUT_DIR / f"{DATASET_NAME}.json"
    with pytest.raises(ValidationError) as e:
        metadata_reader.run_reader(DATASET_NAME, METADATA_PATH)
    assert "Errors found while validating metadata file" in str(e)
    assert e.value.errors == [
        "measureVariables: Can not set a dataType in a measure variable "
        "together with a unitType"
    ]


def test_read_invalid_sensitivity():
    DATASET_NAME = "INVALID_SENSITIVITY"
    METADATA_PATH = INPUT_DIR / f"{DATASET_NAME}.json"
    with pytest.raises(ValidationError) as e:
        metadata_reader.run_reader(DATASET_NAME, METADATA_PATH)
    assert "Errors found while validating metadata file" in str(e)
    assert e.value.errors == [
        "sensitivityLevel: value is not a valid enumeration member; permitted: "
        "'PERSON_GENERAL', 'PERSON_SPECIAL', 'PUBLIC', 'NONPUBLIC'",
        "subjectFields: value is not a valid list",
        "measureVariables->valueDomain->$ref: extra fields not permitted",
    ]


def test_read_invalid_unit_type():
    DATASET_NAME = "INVALID_UNIT_TYPE"
    METADATA_PATH = INPUT_DIR / f"{DATASET_NAME}.json"
    with pytest.raises(ValidationError) as e:
        metadata_reader.run_reader(DATASET_NAME, METADATA_PATH)
    assert "Errors found while validating metadata file" in str(e)
    assert e.value.errors == [
        "identifierVariables->unitType: value is not a valid enumeration member; "
        "permitted: 'JOBB', 'KJORETOY', 'FAMILIE', 'FORETAK', 'HUSHOLDNING', "
        "'KOMMUNE', 'KURS', 'PERSON', 'VIRKSOMHET', 'BK_HELSESTASJONSKONSULTASJON'"
    ]


def test_read_missing_identifier():
    DATASET_NAME = "MISSING_IDENTIFIER"
    METADATA_PATH = INPUT_DIR / f"{DATASET_NAME}.json"
    with pytest.raises(ValidationError) as e:
        metadata_reader.run_reader(DATASET_NAME, METADATA_PATH)
    assert "Errors found while validating metadata file" in str(e)
    assert e.value.errors == ["identifierVariables: field required"]
