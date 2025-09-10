import filecmp
import os
import shutil
import tarfile
from pathlib import Path
from typing import List

from pytest import MonkeyPatch, fail, raises

from microdata_tools import package_dataset, unpackage_dataset
from microdata_tools.packaging._decrypt import _validate_tar_contents
from microdata_tools.packaging.exceptions import (
    InvalidTarFileContents,
    UnpackagingError,
)
from tests.test_packaging.test_package_dataset import _create_rsa_public_key

RSA_KEYS_DIRECTORY = Path("tests/resources/packaging/rsa_test_key")
INPUT_DIRECTORY = Path("tests/resources/packaging/input_unpackage")
OUTPUT_DIRECTORY = Path("tests/resources/packaging/output")


def setup_function():
    shutil.copytree("tests/resources", "tests/resources_backup")


def teardown_function():
    shutil.rmtree("tests/resources")
    shutil.move("tests/resources_backup", "tests/resources")


def test_validate_tar_contents():
    dataset_name = "VALID"
    tarfile_path = INPUT_DIRECTORY / f"{dataset_name}.tar"

    tar = tarfile.open(tarfile_path)

    try:
        _validate_tar_contents(tar.getnames(), dataset_name)
    except InvalidTarFileContents:
        fail("InvalidTarFileContents raised unexpectedly")


def test_validate_tar_contents_only_json():
    dataset_name = "ONLY_JSON"
    tarfile_path = INPUT_DIRECTORY / f"{dataset_name}.tar"

    tar = tarfile.open(tarfile_path)

    try:
        _validate_tar_contents(tar.getnames(), dataset_name)
    except InvalidTarFileContents:
        fail("InvalidTarFileContents raised unexpectedly")


def test_validate_tar_contents_missing_symkey():
    dataset_name = "DATASET_3"
    tarfile_path = INPUT_DIRECTORY / "MISSING_SYMKEY.tar"

    tar = tarfile.open(tarfile_path)

    with raises(InvalidTarFileContents) as e:
        _validate_tar_contents(tar.getnames(), dataset_name)
        assert (
            str(e.value)
            == f"Tar file for {dataset_name} does not contain the required "
            f"{dataset_name}.symkey.encr file"
        )


def test_validate_tar_contents_missing_chunk():
    dataset_name = "DATASET_4"
    tarfile_path = INPUT_DIRECTORY / "MISSING_CHUNK.tar"

    tar = tarfile.open(tarfile_path)

    with raises(InvalidTarFileContents) as e:
        _validate_tar_contents(tar.getnames(), dataset_name)
        assert (
            str(e.value)
            == f"Tar file for {dataset_name} does not contain any chunks files"
        )


def test_unpackage_dataset():
    dataset_name = "VALID"

    tarfile_path = INPUT_DIRECTORY / f"{dataset_name}.tar"

    unpackage_dataset(tarfile_path, RSA_KEYS_DIRECTORY, OUTPUT_DIRECTORY)

    output_dataset_dir = OUTPUT_DIRECTORY / dataset_name
    assert Path(output_dataset_dir / f"{dataset_name}.json").exists()
    assert Path(output_dataset_dir / f"{dataset_name}.csv").exists()
    assert not Path(output_dataset_dir / "chunks/1.csv.encr").exists()
    assert not Path(output_dataset_dir / f"{dataset_name}.symkey.encr").exists()
    assert Path(INPUT_DIRECTORY / f"{dataset_name}.tar").exists()
    assert not Path(INPUT_DIRECTORY / dataset_name).exists()

    actual = Path(output_dataset_dir / f"{dataset_name}.csv")
    expected = Path(
        f"tests/resources/packaging/expected_unpackage/{dataset_name}_expected.csv"
    )
    assert filecmp.cmp(actual, expected)


def test_unpackage_dataset_multiple_chunks(monkeypatch: MonkeyPatch):
    dataset_name = "VALID"
    rsa_key = Path("tests/resources/rsa_keys")

    _create_rsa_public_key(target_dir=rsa_key)
    assert Path(rsa_key / "microdata_public_key.pem").exists()

    # Produces more than 10 chunks
    monkeypatch.setattr(
        "microdata_tools.packaging._encrypt.CHUNK_SIZE_BYTES", 1
    )

    package_dataset(
        rsa_keys_dir=rsa_key,
        dataset_dir=Path(
            f"tests/resources/packaging/input_package/{dataset_name}"
        ),
        output_dir=INPUT_DIRECTORY,
    )

    result_file = INPUT_DIRECTORY / f"{dataset_name}.tar"
    assert result_file.exists()

    unpackage_dataset(result_file, rsa_key, OUTPUT_DIRECTORY)

    output_dataset_dir = OUTPUT_DIRECTORY / dataset_name
    assert Path(output_dataset_dir / f"{dataset_name}.json").exists()
    assert Path(output_dataset_dir / f"{dataset_name}.csv").exists()
    assert not Path(output_dataset_dir / "chunks/1.csv.encr").exists()
    assert not Path(output_dataset_dir / "chunks/2.csv.encr").exists()
    assert not Path(output_dataset_dir / "chunks/3.csv.encr").exists()
    assert not Path(output_dataset_dir / f"{dataset_name}.symkey.encr").exists()
    assert Path(INPUT_DIRECTORY / f"{dataset_name}.tar").exists()
    assert not Path(INPUT_DIRECTORY / dataset_name).exists()

    actual = Path(output_dataset_dir / f"{dataset_name}.csv")
    expected = Path(
        f"tests/resources/packaging/expected_unpackage/{dataset_name}_expected.csv"
    )
    assert filecmp.cmp(actual, expected)


def test_unpackage_dataset_just_json():
    dataset_name = "ONLY_JSON"
    packaged_file_path = INPUT_DIRECTORY / f"{dataset_name}.tar"

    unpackage_dataset(packaged_file_path, RSA_KEYS_DIRECTORY, OUTPUT_DIRECTORY)

    output_dataset_dir = OUTPUT_DIRECTORY / dataset_name
    assert Path(output_dataset_dir / f"{dataset_name}.json").exists()
    assert not Path(output_dataset_dir / f"{dataset_name}.csv").exists()
    assert not Path(output_dataset_dir / f"{dataset_name}.csv.encr").exists()
    assert not Path(output_dataset_dir / f"{dataset_name}.symkey.encr").exists()
    assert Path(INPUT_DIRECTORY / f"{dataset_name}.tar").exists()
    assert not Path(INPUT_DIRECTORY / dataset_name).exists()


def test_unpackage_dataset_failed():
    dataset_name = "MISSING_CHUNK"
    packaged_file_path = INPUT_DIRECTORY / f"{dataset_name}.tar"

    with raises(UnpackagingError):
        unpackage_dataset(
            packaged_file_path, RSA_KEYS_DIRECTORY, OUTPUT_DIRECTORY
        )

    assert Path(INPUT_DIRECTORY / f"{dataset_name}.tar").exists()
    assert not Path(INPUT_DIRECTORY / dataset_name).exists()


def _tar_files(
    packaged_file_path: Path, input_dataset_dir: Path, files_to_tar: List[str]
):
    files_to_tar_as_paths = [
        Path(input_dataset_dir / file) for file in files_to_tar
    ]

    with tarfile.open(packaged_file_path, "w") as tar:
        for file in files_to_tar_as_paths:
            if file.exists():
                tar.add(file, arcname=file.name)
    for file in files_to_tar_as_paths:
        if file.exists():
            os.remove(file)
    shutil.rmtree(input_dataset_dir)
