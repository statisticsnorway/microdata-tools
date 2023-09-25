from pathlib import Path
import string
from pyarrow import parquet, dataset
from typing import List, Union
from microdata_tools.validation.components import unit_id_types
from microdata_tools.validation.exceptions import ValidationError
from microdata_tools.validation.adapter import local_storage
from microdata_tools.validation.steps import (
    metadata_reader,
    dataset_validator,
    data_reader,
    metadata_enricher,
)
from microdata_tools.validation.model.metadata import UnitType, UnitIdType


def _validate_dataset_name(dataset_name: str) -> None:
    """
    Validates that the name of the dataset only contains valid
    characters (uppercase A-Z, numbers 0-9 and _)
    """
    invalid_leading_characters = string.digits + "_"
    valid_characters = string.ascii_uppercase + string.digits + "_"
    if dataset_name[0] in invalid_leading_characters or not all(
        [character in valid_characters for character in dataset_name]
    ):
        raise ValidationError(
            dataset_name,
            [
                f'"{dataset_name}" contains invalid characters. '
                'Please use only uppercase A-Z, numbers 0-9 or "_"',
            ],
        )


def get_unit_id_type_for_unit_type(
    unit_id: UnitType,
) -> Union[None, UnitIdType]:
    """
    Returns the unitIdType for the supplied unitType. Returns None
    if supplied unitType has no attached unitIdType.
    Raises a UnregisteredUnitTypeError on unregistered unitType.
    """
    return unit_id_types.get(unit_id)


def validate_dataset(
    dataset_name: str,
    working_directory: str = "",
    input_directory: str = "",
    keep_temporary_files: bool = False,
) -> List[str]:
    """
    Validate a dataset and return a list of errors.
    If the dataset is valid, the list will be empty.
    """
    data_errors = []
    working_directory_path = None

    try:
        _validate_dataset_name(dataset_name)
        (
            working_directory_path,
            working_directory_was_generated,
        ) = local_storage.resolve_working_directory(working_directory)
        input_dataset_directory = Path(input_directory) / dataset_name
        input_metadata_path = input_dataset_directory / f"{dataset_name}.json"
        input_data_path = input_dataset_directory / f"{dataset_name}.csv"

        # Read and validate metadata
        metadata_dict = metadata_reader.run_reader(
            dataset_name, input_metadata_path
        )
        measure_data_type = metadata_dict["measureVariables"][0]["dataType"]
        identifier_data_type = metadata_dict["identifierVariables"][0][
            "dataType"
        ]
        temporality_type = metadata_dict["temporalityType"]
        code_list = metadata_dict["measureVariables"][0]["valueDomain"].get(
            "codeList"
        )
        sentinel_list = metadata_dict["measureVariables"][0][
            "valueDomain"
        ].get("sentinelAndMissingValues")

        # Read data
        table = data_reader.read_and_sanitize_csv(
            input_data_path,
            identifier_data_type,
            measure_data_type,
            temporality_type,
        )

        # Enrich metadata with temporal data
        temporal_data = data_reader.get_temporal_data(table, temporality_type)
        metadata_enricher.enrich_with_temporal_coverage(
            metadata_dict, temporal_data
        )

        # Write files to working directory
        parquet_path = working_directory_path / f"{dataset_name}.parquet"
        parquet.write_table(table, parquet_path)
        local_storage.write_json(
            working_directory_path / f"{dataset_name}.json", metadata_dict
        )

        # Validate data
        dataset_validator.validate_dataset(
            dataset.dataset(parquet_path),
            measure_data_type,
            code_list,
            sentinel_list,
            temporality_type,
        )
    except ValidationError as e:
        data_errors = e.errors
    except Exception as e:
        # Raise unexpected exceptions to user
        raise e
    finally:
        # Delete temporary files
        if not keep_temporary_files and working_directory_path:
            local_storage.clean_up_temporary_files(
                dataset_name,
                working_directory_path,
                delete_working_directory=working_directory_was_generated,
            )
    return data_errors


def validate_metadata(
    dataset_name: str,
    input_directory: str = "",
    working_directory: str = "",
    keep_temporary_files: bool = False,
) -> List[dict]:
    """
    Validate metadata and return a list of errors.
    If the metadata is valid, the list will be empty.
    """
    data_errors = []
    working_directory_path = None

    try:
        _validate_dataset_name(dataset_name)
        (
            working_directory_path,
            working_directory_was_generated,
        ) = local_storage.resolve_working_directory(working_directory)
        input_dataset_directory = Path(input_directory) / dataset_name
        input_metadata_path = input_dataset_directory / f"{dataset_name}.json"

        metadata_dict = metadata_reader.run_reader(
            dataset_name, input_metadata_path
        )
        local_storage.write_json(
            working_directory_path / f"{dataset_name}.json", metadata_dict
        )
    except ValidationError as e:
        data_errors = e.errors
    except Exception as e:
        # Raise unexpected exceptions to user
        raise e
    finally:
        # Delete temporary files
        if not keep_temporary_files and working_directory_path:
            local_storage.clean_up_temporary_files(
                dataset_name,
                working_directory_path,
                delete_working_directory=working_directory_was_generated,
            )
    return data_errors
