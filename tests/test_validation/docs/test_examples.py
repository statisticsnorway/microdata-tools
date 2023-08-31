from microdata_tools import validate_dataset


INPUT_DIRECTORY = "docs/examples"
EXAMPLE_DATASETS = [
    "BEFOLKNING_KJOENN",
    "BEFOLKNING_SIVILSTAND",
    "BEFOLKNING_INNTEKT",
]


def test_validate_valid_dataset():
    for dataset_name in EXAMPLE_DATASETS:
        data_errors = validate_dataset(
            dataset_name, input_directory=INPUT_DIRECTORY
        )
        assert not data_errors
