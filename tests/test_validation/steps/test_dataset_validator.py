import pytest

from microdata_tools.validation.exceptions import ValidationError
from microdata_tools.validation.steps import dataset_validator
from tests.resources.validation.steps.dataset_validator import test_data


def teardown_module():
    test_data._delete_parquet_files()


def test_measure_code_list_validation():
    code_list = test_data.FIXED_STRING_CODELIST
    sentinel_list = test_data.FIXED_STRING_CODELIST_SENTINEL
    dataset_validator.validate_dataset(
        test_data.FIXED_STRING_CODELIST_DS,
        "STRING",
        code_list,
        sentinel_list,
        "FIXED",
    )
    with pytest.raises(ValidationError) as e:
        dataset_validator.validate_dataset(
            test_data.FIXED_STRING_CODELIST_INVALID_DS,
            "STRING",
            code_list,
            sentinel_list,
            "FIXED",
        )
    assert e.value.errors == ["Error for identifier 4: 3 is not in code list"]


def test_measure_data_type_string_validation():
    dataset_validator.validate_dataset(
        test_data.FIXED_STRING_DS,
        "STRING",
        None,
        None,
        "FIXED",
    )
    with pytest.raises(ValidationError) as e:
        dataset_validator.validate_dataset(
            test_data.FIXED_STRING_INVALID_DS,
            "STRING",
            None,
            None,
            "FIXED",
        )
    assert e.value.errors == [
        "Invalid value in #2 column for row with identifier: 2",
        "Invalid value in #2 column for row with identifier: 3",
    ]


def test_measure_data_type_long_validation():
    dataset_validator.validate_dataset(
        test_data.FIXED_LONG_DS,
        "LONG",
        None,
        None,
        "FIXED",
    )
    with pytest.raises(ValidationError) as e:
        dataset_validator.validate_dataset(
            test_data.FIXED_LONG_INVALID_DS,
            "LONG",
            None,
            None,
            "FIXED",
        )
    assert e.value.errors == [
        "Invalid value in #2 column for row with identifier: 4"
    ]


def test_measure_data_type_double_validation():
    dataset_validator.validate_dataset(
        test_data.FIXED_DOUBLE_DS,
        "DOUBLE",
        None,
        None,
        "FIXED",
    )
    with pytest.raises(ValidationError) as e:
        dataset_validator.validate_dataset(
            test_data.FIXED_DOUBLE_INVALID_DS,
            "DOUBLE",
            None,
            None,
            "FIXED",
        )
    assert e.value.errors == [
        "Invalid value in #2 column for row with identifier: 4"
    ]


def test_measure_data_type_date_validation():
    dataset_validator.validate_dataset(
        test_data.FIXED_DATE_DS,
        "DATE",
        None,
        None,
        "FIXED",
    )
    with pytest.raises(ValidationError) as e:
        dataset_validator.validate_dataset(
            test_data.FIXED_DATE_INVALID_DS,
            "DATE",
            None,
            None,
            "FIXED",
        )
    assert e.value.errors == [
        "Invalid value in #2 column for row with identifier: 4"
    ]


def test_temporality_fixed():
    dataset_validator.validate_dataset(
        test_data.FIXED_VALID_DS,
        "STRING",
        None,
        None,
        "FIXED",
    )
    with pytest.raises(ValidationError) as e:
        dataset_validator.validate_dataset(
            test_data.FIXED_INVALID_START_DS,
            "STRING",
            None,
            None,
            "FIXED",
        )
    assert e.value.errors == [
        f"Invalid #3 and/or #4 columns for row with identifier: {id}"
        for id in range(1, 5)
    ]
    with pytest.raises(ValidationError) as e:
        dataset_validator.validate_dataset(
            test_data.FIXED_INVALID_DUPLICATES_DS,
            "STRING",
            None,
            None,
            "FIXED",
        )
    assert e.value.errors == ["Duplicate identifiers in #1 column"]


def test_temporality_status():
    dataset_validator.validate_dataset(
        test_data.STATUS_VALID_DS,
        "STRING",
        None,
        None,
        "STATUS",
    )
    with pytest.raises(ValidationError) as e:
        dataset_validator.validate_dataset(
            test_data.STATUS_INVALID_START_STOP_DS,
            "STRING",
            None,
            None,
            "STATUS",
        )
    assert e.value.errors == [
        f"#3 column not equal to #4 column for row with identifier: {id}"
        for id in range(1, 5)
    ]
    with pytest.raises(ValidationError) as e:
        dataset_validator.validate_dataset(
            test_data.STATUS_INVALID_DUPLICATES_DS,
            "STRING",
            None,
            None,
            "STATUS",
        )
    assert e.value.errors == [
        "Same unit_id (#1 Column) has duplicate dates (#3 and #4 column)"
    ]


def test_temporality_event():
    dataset_validator.validate_dataset(
        test_data.EVENT_VALID_DS,
        "STRING",
        None,
        None,
        "EVENT",
    )
    with pytest.raises(ValidationError) as e:
        dataset_validator.validate_dataset(
            test_data.EVENT_INVALID_START_DS,
            "STRING",
            None,
            None,
            "EVENT",
        )
    assert e.value.errors == [
        "Invalid #3 and/or #4 columns for row with identifier: 1"
    ]
    with pytest.raises(ValidationError) as e:
        dataset_validator.validate_dataset(
            test_data.EVENT_INVALID_TIMESPANS_DS,
            "STRING",
            None,
            None,
            "EVENT",
        )
    assert e.value.errors == [
        (
            'Invalid overlapping timespans for identifier "1": '
            "timespan: (2020-12-30 - 2021-02-12) overlaps with "
            "timespan: (2020-12-31 - 2021-02-22)"
        )
    ]
    with pytest.raises(ValidationError) as e:
        dataset_validator.validate_dataset(
            test_data.EVENT_TOO_MANY_ERRORS_DS,
            "STRING",
            None,
            None,
            "EVENT",
        )
    assert len(e.value.errors) == 50


def test_temporality_accumulated():
    dataset_validator.validate_dataset(
        test_data.ACCUMULATED_VALID_DS,
        "STRING",
        None,
        None,
        "ACCUMULATED",
    )
    with pytest.raises(ValidationError) as e:
        dataset_validator.validate_dataset(
            test_data.ACCUMULATED_INVALID_START_STOP_DS,
            "STRING",
            None,
            None,
            "ACCUMULATED",
        )
    assert e.value.errors == [
        "Invalid #3 and/or #4 columns for row with identifier: 1",
        "Invalid #3 and/or #4 columns for row with identifier: 1",
        "Invalid #3 and/or #4 columns for row with identifier: 2",
    ]
    with pytest.raises(ValidationError) as e:
        dataset_validator.validate_dataset(
            test_data.ACCUMULATED_INVALID_TIMESPANS_DS,
            "STRING",
            None,
            None,
            "ACCUMULATED",
        )
    print(e.value.errors)
    assert e.value.errors == [
        (
            'Invalid overlapping timespans for identifier "1": '
            "timespan: (2020-12-30 - 2021-02-12) overlaps with "
            "timespan: (2020-12-31 - 2021-02-14)"
        ),
    ]


def test_max_50_errors():
    # codelist error
    with pytest.raises(ValidationError) as e:
        dataset_validator.validate_dataset(
            test_data.TOO_MANY_ERRORS_DS,
            "STRING",
            test_data.TOO_MANY_ERRORS_CODELIST,
            None,
            "ACCUMULATED",
        )
    assert len(e.value.errors) == 50

    # temporal error
    with pytest.raises(ValidationError) as e:
        dataset_validator.validate_dataset(
            test_data.TOO_MANY_ERRORS_DS,
            "STRING",
            None,
            None,
            "FIXED",
        )
        assert len(e.value.errors) == 50
