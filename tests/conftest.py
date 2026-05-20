import os
from pathlib import Path

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import mlkem, x25519


@pytest.fixture
def output_dir(tmp_path: Path) -> Path:
    return tmp_path / "output"


@pytest.fixture
def keys_dir(tmp_path: Path):
    keys_dir = tmp_path / "keys_dir"
    write_combined_mlkem_x25519_key_files(keys_dir)
    return keys_dir


def write_combined_mlkem_x25519_key_files(target_dir: Path) -> None:
    if not target_dir.exists():
        os.makedirs(target_dir)
    mlkem_private_key = mlkem.MLKEM768PrivateKey.generate()
    x25519_private_key = x25519.X25519PrivateKey.generate()

    mlkem_priv_pem = mlkem_private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    x25519_priv_pem = x25519_private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )

    mlkem_pub_pem = mlkem_private_key.public_key().public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    x25519_pub_pem = x25519_private_key.public_key().public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )

    with open(target_dir / "microdata_public_key.pem", "wb") as file:
        file.write(mlkem_pub_pem)
        file.write(x25519_pub_pem)

    with open(target_dir / "microdata_private_key.pem", "wb") as file:
        file.write(mlkem_priv_pem)
        file.write(x25519_priv_pem)


def pytest_addoption(parser):
    parser.addoption(
        "--include-big-data",
        action="store_true",
        dest="include-big-data",
        default=False,
        help="enable big data testing",
    )
