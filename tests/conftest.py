import os
from pathlib import Path

import pytest

from microdata_tools.keys import PrivateKey


@pytest.fixture
def output_dir(tmp_path: Path) -> Path:
    return tmp_path / "output"


@pytest.fixture
def keys_dir(tmp_path: Path) -> Path:
    keys_dir = tmp_path / "keys_dir"
    generate_keys(keys_dir)
    return keys_dir


def generate_keys(keys_dir: Path) -> None:
    if not keys_dir.exists():
        os.makedirs(keys_dir)
    private_key = PrivateKey.generate()
    private_key.write_to_file(keys_dir)
    private_key.public_key().write_to_file(keys_dir)


def pytest_addoption(parser):
    parser.addoption(
        "--include-big-data",
        action="store_true",
        dest="include-big-data",
        default=False,
        help="enable big data testing",
    )
