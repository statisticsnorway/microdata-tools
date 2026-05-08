import os
from pathlib import Path

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import mlkem


@pytest.fixture
def mlkem_keys_dir(tmp_path: Path):
    keys_dir = tmp_path / "mlkem_keys"
    if not keys_dir.exists():
        os.makedirs(keys_dir)

    private_key = mlkem.MLKEM768PrivateKey.generate()
    public_key = private_key.public_key()

    microdata_public_key_pem = public_key.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )

    public_key_location = keys_dir / "microdata_public_key.pem"
    with open(public_key_location, "wb") as file:
        file.write(microdata_public_key_pem)

    with open(keys_dir / "microdata_private_key.pem", "wb") as file:
        file.write(
            private_key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption(),
            )
        )
    return keys_dir


@pytest.fixture
def output_dir(tmp_path: Path) -> Path:
    return tmp_path / "output"


def pytest_addoption(parser):
    parser.addoption(
        "--include-big-data",
        action="store_true",
        dest="include-big-data",
        default=False,
        help="enable big data testing",
    )
