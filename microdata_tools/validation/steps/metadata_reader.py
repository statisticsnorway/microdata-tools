from pathlib import Path
from typing import Dict

from microdata_tools.validation.model import validate_metadata_model
from microdata_tools.validation.adapter import local_storage
from microdata_tools.validation.components import (
    temporal_attributes,
    unit_type_variables,
)


def _insert_centralized_variable_definitions(metadata: dict):
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
    metadata_dict["shortName"] = dataset_name
    metadata_dict["measureVariables"][0]["shortName"] = dataset_name
    return metadata_dict
