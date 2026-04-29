import logging

import pytest

from microdata_tools import validate_dataset
from tests import log_setup

logger = logging.getLogger()

INPUT_DIRECTORY = "docs/examples"
EXAMPLE_DATASETS = [
    "BEFOLKNING_KJOENN",
    "BEFOLKNING_SIVILSTAND",
    "BEFOLKNING_INNTEKT",
    "BEFOLKNING_BOSTED",
]


@pytest.mark.focus
def test_validate_valid_dataset():
    log_setup.init_logging()
    for dataset_name in EXAMPLE_DATASETS:
        data_errors = validate_dataset(
            dataset_name, input_directory=INPUT_DIRECTORY
        )
        logger.info(f"errors: {data_errors}")
        assert not data_errors
