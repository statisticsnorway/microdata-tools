from pathlib import Path
from datetime import datetime
from typing import Dict, Union, List

from microdata_tools.validation.model import validate_metadata_model
from microdata_tools.validation.adapter import local_storage
from microdata_tools.validation.components import (
    temporal_attributes,
    unit_type_variables,
)
from microdata_tools.validation.exceptions import ValidationError


def _days_since_epoch(date_string: str) -> int:
    epoch = datetime.utcfromtimestamp(0)
    date_obj = datetime.strptime(date_string, "%Y-%m-%d")
    return (date_obj - epoch).days


def _validate_code_list(
    code_list: List[Dict[str, str]],
) -> List[str]:
    errors = []
    if not code_list:
        return ["Code list can not be empty"]

    ONE_DAY = 1

    has_ongoing_time_period = False
    valid_from_dates = []
    valid_until_dates = []
    for code in code_list:
        valid_from_dates.append(_days_since_epoch(code.get("validFrom")))
        if code.get("validUntil") is not None:
            valid_until_dates.append(
                _days_since_epoch(code.get("validUntil")) + ONE_DAY
            )
        else:
            has_ongoing_time_period = True

    unique_dates = list(set(valid_from_dates + valid_until_dates))
    unique_dates.sort()

    valid_periods = []
    for i, _ in enumerate(unique_dates):
        if i < len(unique_dates) - 1:
            valid_periods.append(
                (unique_dates[i], unique_dates[i + 1] - ONE_DAY)
            )
        else:
            valid_periods.append((unique_dates[i], None))

    if not has_ongoing_time_period:
        valid_periods = valid_periods[:-1]

    for valid_period in valid_periods:
        period_code_list = []
        for code in code_list:
            code_valid_from = _days_since_epoch(code.get("validFrom"))
            code_valid_until = (
                None
                if code.get("validUntil") is None
                else _days_since_epoch(code.get("validUntil"))
            )
            valid_period_is_ongoing: bool = valid_period[1] is None
            code_period_is_ongoing: bool = code_valid_until is None
            code_period_started_before_valid_period: bool = (
                code_valid_from <= valid_period[0]
            )
            valid_period_inside_code_period: bool = (
                not valid_period_is_ongoing
                and not code_period_is_ongoing
                and code_valid_from <= valid_period[0]
                and valid_period[1] <= code_valid_until
            )
            code_in_valid_period: bool = (
                code_period_is_ongoing
                and code_period_started_before_valid_period
            ) or (valid_period_inside_code_period)
            if code_in_valid_period:
                period_code_list.append(code["code"])
        duplicate_codes = [
            code
            for code in period_code_list
            if period_code_list.count(code) > 1
        ]
        if duplicate_codes:
            errors.append(
                f"Duplicate codes for same time period: {list(set(duplicate_codes))}"
            )
    return errors


def _validate_code_lists(metadata: Dict):
    measure_value_domain: Union[Dict, None] = metadata.get(
        "measureVariables", [{}]
    )[0].get("valueDomain")
    if measure_value_domain and measure_value_domain.get("codeList"):
        code_list_errors = _validate_code_list(
            measure_value_domain["codeList"]
        )
        if code_list_errors:
            return code_list_errors
    identifier_value_domain: Union[Dict, None] = metadata.get(
        "identifierVariables", [{}]
    )[0].get("valueDomain")
    if identifier_value_domain and identifier_value_domain.get("codeList"):
        code_list_errors = _validate_code_list(
            identifier_value_domain["codeList"]
        )
        if code_list_errors:
            return code_list_errors
    return []


def _insert_centralized_variable_definitions(metadata: Dict):
    metadata["identifierVariables"] = [
        unit_type_variables.get(metadata["identifierVariables"][0]["unitType"])
    ]
    measure_variable = metadata["measureVariables"][0]
    if "unitType" in measure_variable:
        insert_measure = unit_type_variables.get(measure_variable["unitType"])
        insert_measure["name"] = measure_variable["name"]
        insert_measure["description"] = measure_variable["description"]
        metadata["measureVariables"] = [insert_measure]
    temporality_type = metadata["temporalityType"]
    metadata["attributeVariables"] = [
        temporal_attributes.generate_start_time_attribute(temporality_type),
        temporal_attributes.generate_stop_time_attribute(temporality_type),
    ] + metadata.get("attributeVariables", [])


def run_reader(dataset_name: str, metadata_file_path: Path) -> Dict:
    metadata_dict = local_storage.load_json(metadata_file_path)
    validate_metadata_model(metadata_dict)
    _insert_centralized_variable_definitions(metadata_dict)
    code_list_errors = _validate_code_lists(metadata_dict)
    if code_list_errors:
        raise ValidationError(
            "Errors found in code list", errors=code_list_errors
        )
    metadata_dict["shortName"] = dataset_name
    metadata_dict["measureVariables"][0]["shortName"] = dataset_name
    return metadata_dict
