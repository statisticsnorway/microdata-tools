import os
from pathlib import Path
import shutil
import tarfile
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.backends import default_backend

from microdata_tools import package_dataset

RSA_KEYS_DIRECTORY = Path("tests/resources/rsa_keys")
INPUT_DIRECTORY = Path("tests/resources/input_package")
OUTPUT_DIRECTORY = Path("tests/resources/output")


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

    with tarfile.open(result_file, "r:") as tar:
        tarred_files = [file.name for file in tar.getmembers()]
        assert len(tarred_files) == 3
        assert f"{dataset_name}_chunk_1.csv.encr" in tarred_files
        assert f"{dataset_name}.symkey.encr" in tarred_files
        assert f"{dataset_name}.json" in tarred_files


def test_package_dataset_multiple_chunks():
    dataset_name = "VALID"

    _create_rsa_public_key(target_dir=RSA_KEYS_DIRECTORY)

    package_dataset(
        rsa_keys_dir=RSA_KEYS_DIRECTORY,
        dataset_dir=Path(f"{INPUT_DIRECTORY}/{dataset_name}"),
        output_dir=OUTPUT_DIRECTORY,
        chunk_size_bytes=5,
    )

    result_file = OUTPUT_DIRECTORY / f"{dataset_name}.tar"
    assert result_file.exists()

    assert not Path(OUTPUT_DIRECTORY / f"{dataset_name}").exists()

    with tarfile.open(result_file, "r:") as tar:
        tarred_files = [file.name for file in tar.getmembers()]
        assert len(tarred_files) == 5
        assert f"{dataset_name}_chunk_1.csv.encr" in tarred_files
        assert f"{dataset_name}_chunk_2.csv.encr" in tarred_files
        assert f"{dataset_name}_chunk_3.csv.encr" in tarred_files
        assert f"{dataset_name}.symkey.encr" in tarred_files
        assert f"{dataset_name}.json" in tarred_files


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
