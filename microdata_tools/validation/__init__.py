import logging
import string
import time
from pathlib import Path
from typing import List, Union

from pyarrow import dataset, parquet

from microdata_tools.validation.adapter import local_storage
from microdata_tools.validation.components import unit_id_types
from microdata_tools.validation.exceptions import ValidationError
from microdata_tools.validation.model.metadata import UnitIdType, UnitType
from microdata_tools.validation.steps import (
    data_reader,
    dataset_validator,
    metadata_enricher,
    metadata_reader,
)

logger = logging.getLogger()


def current_milli_time() -> int:
    return time.time_ns() // 1_000_000


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
    working_directory_was_generated = False

    try:
        start_total_ms = current_milli_time()
        _validate_dataset_name(dataset_name)
        local_storage.validate_dataset_dir(Path(input_directory), dataset_name)
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
        sentinel_list = metadata_dict["measureVariables"][0]["valueDomain"].get(
            "sentinelAndMissingValues"
        )

        file_size = input_data_path.stat().st_size

        # Read data
        table = data_reader.read_and_sanitize_csv(
            input_data_path,
            identifier_data_type,
            measure_data_type,
            temporality_type,
        )
        logger.info("")

        # Enrich metadata with temporal data
        temporal_data = data_reader.get_temporal_data(table, temporality_type)
        metadata_enricher.enrich_with_temporal_coverage(
            metadata_dict, temporal_data
        )

        # Write files to working directory
        start_ms = current_milli_time()
        parquet_path = working_directory_path / f"{dataset_name}.parquet"
        parquet.write_table(table, parquet_path)
        spent_ms = current_milli_time() - start_ms
        logger.info("")
        logger.info(f"parquet.write_table spent: {spent_ms:_} ms")
        logger.info(
            f"parquet.write_table speed: {
                (file_size / 1024 / 1024) / (spent_ms / 1000):.1f} MB/s"
        )

        # start_ms_ = current_milli_time()
        local_storage.write_json(
            working_directory_path / f"{dataset_name}.json", metadata_dict
        )
        # spent_ms_ = current_milli_time() - start_ms_
        # logger.info(f'local_storage.write_json spent: {spent_ms_:_} ms')

        # Validate data
        start_ms_validate = current_milli_time()
        if False:
            dataset_validator.validate_dataset(
                dataset.dataset(parquet_path),
                measure_data_type,
                code_list,
                sentinel_list,
                temporality_type,
            )
            spent_ms = current_milli_time() - start_ms_validate
            logger.info(
                f"dataset_validator.validate_dataset spent: {spent_ms:_} ms"
            )
            logger.info(
                f"dataset_validator.validate_dataset speed: {
                    (file_size / 1024 / 1024) / (spent_ms / 1000):.1f} MB/s"
            )

        spent_total_ms = current_milli_time() - start_total_ms
        logger.info("")
        logger.info(f"total validate_dataset spent: {spent_total_ms:_} ms")
        logger.info(
            f"total validate_dataset file size: {
                file_size // 1024 // 1024:_} MB"
        )
        logger.info(
            f"total validate_dataset speed: {
                (file_size / 1024 / 1024) / (spent_total_ms / 1000):.1f} MB/s"
        )
        logger.info("")
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
) -> list[str]:
    """
    Validate metadata and return a list of errors.
    If the metadata is valid, the list will be empty.
    """
    data_errors = []
    working_directory_path = None
    working_directory_was_generated = False

    try:
        _validate_dataset_name(dataset_name)
        local_storage.validate_dataset_dir(
            Path(input_directory), dataset_name, require_csv=False
        )
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
