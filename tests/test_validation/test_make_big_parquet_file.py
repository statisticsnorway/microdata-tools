import logging
import os
import shutil
import uuid
from pathlib import Path

import pytest

from microdata_tools import validate_dataset
from microdata_tools.validation.steps import old_init
from microdata_tools.validation.steps.utils import current_milli_time, ms_to_eta
from tests import log_setup

logger = logging.getLogger()
RESOURCE_DIR = "tests/resources/validation/validate_dataset/big_datasets"

VALID_DATASET_NAMES = [
    "ACCUMULATED_DS",
    # "EVENT_DS",
    # "FIXED_DS",
    # "STATUS_DS",
]


def setup_function():
    log_setup.init_logging()
    for dataset_name in VALID_DATASET_NAMES:
        csv_file_path = f"{RESOURCE_DIR}/{dataset_name}/{dataset_name}.csv"
        parquet_file_path = (
            f"{RESOURCE_DIR}/{dataset_name}/{dataset_name}.parquet"
        )
        parquet_done_file_path = (
            f"{RESOURCE_DIR}/{dataset_name}/{dataset_name}.parquet.done"
        )
        logger.info(f"Parquet file path: {parquet_file_path}")
        logger.info(f"Parquet done file path: {parquet_done_file_path}")
        assert os.path.exists(csv_file_path)
        create = False
        if not os.path.exists(parquet_done_file_path):
            create = True
        elif not os.path.exists(parquet_file_path):
            create = True
        else:
            f1 = os.path.getmtime(csv_file_path)
            f2 = os.path.getmtime(parquet_done_file_path)
            f3 = os.path.getmtime(parquet_file_path)
            if f1 > f2:
                create = True
            if f1 > f3:
                create = True
        if create:
            logger.info("Creating parquet file ...")
            log_setup.init_logging()
            logger.info(f"self pid is {os.getpid()}")
            working_directory = Path("workdir/" + str(uuid.uuid4()))
            os.makedirs(working_directory)

            logger.info(f"Main worker pid is: {str(os.getpid())}")
            try:
                data_errors = validate_dataset(
                    dataset_name,
                    working_directory=working_directory,
                    keep_temporary_files=False,
                    input_directory=RESOURCE_DIR,
                )
                with open(parquet_done_file_path, mode="w") as f:
                    f.write("done")
                assert not data_errors
            finally:
                try:
                    shutil.rmtree(working_directory)
                except Exception:
                    pass
        else:
            logger.info("Parquet file up to date")


@pytest.mark.create_parquet
def test_create_parquet():
    log_setup.init_logging()


@pytest.mark.validate_parquet
def test_validate_parquet():
    log_setup.init_logging()

    working_directory = Path("workdir/" + str(uuid.uuid4()))
    os.makedirs(working_directory)

    try:
        for idx, dataset_name in enumerate(VALID_DATASET_NAMES):
            start_time = current_milli_time()
            if idx != 0:
                logger.info("")
            logger.info(f"OLD Begin {dataset_name} ...")
            data_errors = old_init.validate_dataset_parquet(
                dataset_name,
                working_directory=working_directory,
                keep_temporary_files=False,
                input_directory=RESOURCE_DIR,
            )
            spent_ms = current_milli_time() - start_time
            assert not data_errors
            logger.info(
                f"Done {dataset_name}. Spent: {spent_ms:_} ms "
                + f"aka {ms_to_eta(spent_ms)}"
            )
    finally:
        try:
            shutil.rmtree(working_directory)
        except Exception:
            pass
