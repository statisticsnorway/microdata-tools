import pytest

from microdata_tools.validation import _validate_dataset_name
from microdata_tools.validation.exceptions import ValidationError


def test_validate_dataset_name():
    _validate_dataset_name("MITT_DATASET")
    _validate_dataset_name("ABCDEFGHIJKLMNOPQRSTUVWXYZ")
    _validate_dataset_name("A123456789")
    _validate_dataset_name("B1A_2Z3_334_567_GHJ")

    with pytest.raises(ValidationError):
        _validate_dataset_name("_LEADING_UNDERSCORE")
    with pytest.raises(ValidationError):
        _validate_dataset_name("1LEADING_NUMBER")
    with pytest.raises(ValidationError):
        _validate_dataset_name("ÆØÅ")
    with pytest.raises(ValidationError):
        _validate_dataset_name("MY-!DÅTÆSØT-!?")
    with pytest.raises(ValidationError):
        _validate_dataset_name("MY.DATASET-!?")
