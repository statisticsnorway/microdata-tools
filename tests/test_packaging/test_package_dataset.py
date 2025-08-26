import os
import shutil
import tarfile
from pathlib import Path

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from pytest import MonkeyPatch

from microdata_tools import package_dataset

RSA_KEYS_DIRECTORY = Path("tests/resources/packaging/rsa_keys")
INPUT_DIRECTORY = Path("tests/resources/packaging/input_package")
OUTPUT_DIRECTORY = Path("tests/resources/packaging/output")


def setup_function():
    shutil.copytree("tests/resources", "tests/resources_backup")


def teardown_function():
    shutil.rmtree("tests/resources")
    shutil.move("tests/resources_backup", "tests/resources")


def test_package_dataset():
    dataset_name = "VALID"

    _create_rsa_public_key(target_dir=RSA_KEYS_DIRECTORY)

    package_dataset(
        rsa_keys_dir=RSA_KEYS_DIRECTORY,
        dataset_dir=Path(f"{INPUT_DIRECTORY}/{dataset_name}"),
        output_dir=OUTPUT_DIRECTORY,
    )

    result_file = OUTPUT_DIRECTORY / f"{dataset_name}.tar"
    assert result_file.exists()

    assert not Path(OUTPUT_DIRECTORY / f"{dataset_name}").exists()
    assert not Path(
        INPUT_DIRECTORY / f"{dataset_name}" / f"{dataset_name}.md5"
    ).exists()

    with tarfile.open(result_file, "r:") as tar:
        tarred_files = [file.name for file in tar.getmembers()]
        assert (
            len(tarred_files) == 5
        )  # the chunk dir adds an extra "file" when peeking
        assert "chunks/1.csv.encr" in tarred_files
        assert f"{dataset_name}.symkey.encr" in tarred_files
        assert f"{dataset_name}.json" in tarred_files
        assert f"{dataset_name}.md5" in tarred_files


def test_package_dataset_multiple_chunks(monkeypatch: MonkeyPatch):
    dataset_name = "VALID"

    _create_rsa_public_key(target_dir=RSA_KEYS_DIRECTORY)

    monkeypatch.setattr(
        "microdata_tools.packaging._encrypt.CHUNK_SIZE_BYTES", 5
    )

    package_dataset(
        rsa_keys_dir=RSA_KEYS_DIRECTORY,
        dataset_dir=Path(f"{INPUT_DIRECTORY}/{dataset_name}"),
        output_dir=OUTPUT_DIRECTORY,
    )

    result_file = OUTPUT_DIRECTORY / f"{dataset_name}.tar"
    assert result_file.exists()

    assert not Path(OUTPUT_DIRECTORY / f"{dataset_name}").exists()
    assert not Path(
        INPUT_DIRECTORY / f"{dataset_name}" / f"{dataset_name}.md5"
    ).exists()

    with tarfile.open(result_file, "r:") as tar:
        tarred_files = [file.name for file in tar.getmembers()]
        assert (
            len(tarred_files) == 7
        )  # the chunk dir adds an extra "file" when peeking
        assert "chunks/1.csv.encr" in tarred_files
        assert "chunks/2.csv.encr" in tarred_files
        assert "chunks/3.csv.encr" in tarred_files
        assert f"{dataset_name}.symkey.encr" in tarred_files
        assert f"{dataset_name}.json" in tarred_files
        assert f"{dataset_name}.md5" in tarred_files


def test_package_dataset_just_json():
    dataset_name = "ONLY_JSON"
    _create_rsa_public_key(target_dir=RSA_KEYS_DIRECTORY)

    package_dataset(
        rsa_keys_dir=RSA_KEYS_DIRECTORY,
        dataset_dir=Path(f"{INPUT_DIRECTORY}/{dataset_name}"),
        output_dir=OUTPUT_DIRECTORY,
    )

    result_file = OUTPUT_DIRECTORY / f"{dataset_name}.tar"
    assert result_file.exists()

    assert not Path(OUTPUT_DIRECTORY / f"{dataset_name}").exists()

    with tarfile.open(result_file, "r:") as tar:
        tarred_files = [file.name for file in tar.getmembers()]
        assert len(tarred_files) == 1
        assert f"{dataset_name}.json" in tarred_files


def _create_rsa_public_key(target_dir: Path):
    if not target_dir.exists():
        os.makedirs(target_dir)

    private_key = rsa.generate_private_key(
        public_exponent=65537, key_size=2048, backend=default_backend()
    )

    public_key = private_key.public_key()

    microdata_public_key_pem = public_key.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )

    public_key_location = target_dir / "microdata_public_key.pem"
    with open(public_key_location, "wb") as file:
        file.write(microdata_public_key_pem)

    with open(target_dir / "microdata_private_key.pem", "wb") as file:
        file.write(
            private_key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.TraditionalOpenSSL,
                encryption_algorithm=serialization.NoEncryption(),
            )
        )
