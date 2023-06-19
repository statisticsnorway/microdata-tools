from pathlib import Path

from microdata_tools.exceptions import ValidationException


def check_exists(path: Path):
    if not path.exists():
        raise ValidationException(f"The path {path} does not exist")
