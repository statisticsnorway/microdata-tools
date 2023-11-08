import json
import logging
import os
import shutil
from pathlib import Path
from typing import Tuple, Union
import uuid


logger = logging.getLogger()


def load_json(filepath: Path) -> dict:
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logging.error(f"Failed to open file at {filepath}")
        raise e


def write_json(filepath: Path, content: dict) -> None:
    with open(filepath, "w", encoding="utf-8") as json_file:
        json.dump(content, json_file, indent=4, ensure_ascii=False)


def resolve_working_directory(
    working_directory: Union[str, None]
) -> Tuple[Path, bool]:
    """
    Generates a working directory if a working directory is not supplied.
    Returns a tuple with:
        * The working directory Path
        * True, if directory was generated. False if not.
    """
    if working_directory:
        return Path(working_directory), False
    else:
        generated_working_directory = Path(str(uuid.uuid4()))
        os.mkdir(generated_working_directory)
        return generated_working_directory, True


def clean_up_temporary_files(
    dataset_name: str,
    working_directory: Path,
    delete_working_directory: Path = False,
):
    generated_files = [
        f"{dataset_name}.parquet",
        f"{dataset_name}.json",
    ]
    if delete_working_directory:
        temporary_files = os.listdir(working_directory)
        unknown_files = [
            file for file in temporary_files if file not in generated_files
        ]
        if not unknown_files:
            try:
                shutil.rmtree(working_directory)
            except Exception as e:
                logger.error(
                    "An exception occured while attempting to delete"
                    f"temporary files: {e}"
                )
                raise e
        else:
            for file in generated_files:
                try:
                    os.remove(working_directory / file)
                except FileNotFoundError as e:
                    logger.error(
                        f"Could not find file {file} in working directory "
                        "when attempting to delete temporary files."
                    )
                    raise e
    else:
        for file in generated_files:
            try:
                os.remove(working_directory / file)
            except FileNotFoundError as e:
                logger.error(
                    f"Could not find file {file} in working directory "
                    "when attempting to delete temporary files."
                )
                raise e
