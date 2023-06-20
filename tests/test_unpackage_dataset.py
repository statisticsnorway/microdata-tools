import filecmp
import os
import shutil
import tarfile
from pathlib import Path
from typing import List

import pytest

from microdata_tools import unpackage_dataset
from microdata_tools._decrypt import _validate_tar_contents
from microdata_tools.exceptions import InvalidTarFileContents


RSA_KEYS_DIRECTORY = Path("tests/resources/rsa_test_key")
INPUT_DIRECTORY = Path("tests/resources/input_unpackage")
OUTPUT_DIRECTORY = Path("tests/resources/output")
ARCHIVE_DIR = Path("tests/resources/archive")


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
        pytest.fail("InvalidTarFileContents raised unexpectedly")


def test_validate_tar_contents_only_json():
    dataset_name = "ONLY_JSON"
    tarfile_path = INPUT_DIRECTORY / f"{dataset_name}.tar"

    tar = tarfile.open(tarfile_path)

    try:
        _validate_tar_contents(tar.getnames(), dataset_name)
    except InvalidTarFileContents:
        pytest.fail("InvalidTarFileContents raised unexpectedly")


def test_validate_tar_contents_missing_symkey():
    dataset_name = "DATASET_3"
    tarfile_path = INPUT_DIRECTORY / "MISSING_SYMKEY.tar"

    tar = tarfile.open(tarfile_path)

    with pytest.raises(InvalidTarFileContents) as e:
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

    with pytest.raises(InvalidTarFileContents) as e:
        _validate_tar_contents(tar.getnames(), dataset_name)
        assert (
            str(e.value)
            == f"Tar file for {dataset_name} does not contain any chunks files"
        )


def test_unpackage_dataset():
    dataset_name = "VALID"

    tarfile_path = INPUT_DIRECTORY / f"{dataset_name}.tar"

    # tar = tarfile.open(tarfile_path)
    # print("Tar members: ", tar.getnames())

    unpackage_dataset(tarfile_path, RSA_KEYS_DIRECTORY, OUTPUT_DIRECTORY, ARCHIVE_DIR)

    output_dataset_dir = OUTPUT_DIRECTORY / dataset_name
    assert Path(output_dataset_dir / f"{dataset_name}.json").exists()
    assert Path(output_dataset_dir / f"{dataset_name}.csv").exists()
    assert not Path(output_dataset_dir / f"{dataset_name}_chunk_1.csv.encr").exists()
    assert not Path(output_dataset_dir / f"{dataset_name}.symkey.encr").exists()
    assert not Path(INPUT_DIRECTORY / f"{dataset_name}.tar").exists()
    assert not Path(INPUT_DIRECTORY / dataset_name).exists()
    assert Path(ARCHIVE_DIR / "unpackaged" / f"{dataset_name}.tar").exists()

    actual = Path(output_dataset_dir / f"{dataset_name}.csv")
    expected = Path(f"tests/resources/expected_unpackage/{dataset_name}_expected.csv")
    assert filecmp.cmp(actual, expected)


def test_unpackage_dataset_just_json():
    dataset_name = "ONLY_JSON"
    packaged_file_path = INPUT_DIRECTORY / f"{dataset_name}.tar"

    unpackage_dataset(
        packaged_file_path, RSA_KEYS_DIRECTORY, OUTPUT_DIRECTORY, ARCHIVE_DIR
    )

    output_dataset_dir = OUTPUT_DIRECTORY / dataset_name
    assert Path(output_dataset_dir / f"{dataset_name}.json").exists()
    assert not Path(output_dataset_dir / f"{dataset_name}.csv").exists()
    assert not Path(output_dataset_dir / f"{dataset_name}.csv.encr").exists()
    assert not Path(output_dataset_dir / f"{dataset_name}.symkey.encr").exists()
    assert not Path(INPUT_DIRECTORY / f"{dataset_name}.tar").exists()
    assert not Path(INPUT_DIRECTORY / dataset_name).exists()
    assert Path(ARCHIVE_DIR / "unpackaged" / f"{dataset_name}.tar").exists()


def test_unpackage_dataset_failed():
    dataset_name = "MISSING_CHUNK"
    packaged_file_path = INPUT_DIRECTORY / f"{dataset_name}.tar"

    unpackage_dataset(
        packaged_file_path, RSA_KEYS_DIRECTORY, OUTPUT_DIRECTORY, ARCHIVE_DIR
    )

    assert not Path(INPUT_DIRECTORY / f"{dataset_name}.tar").exists()
    assert not Path(INPUT_DIRECTORY / dataset_name).exists()
    assert Path(ARCHIVE_DIR / "failed" / f"{dataset_name}.tar").exists()


def _tar_files(
    packaged_file_path: Path, input_dataset_dir: Path, files_to_tar: List[str]
):
    files_to_tar_as_paths = [Path(input_dataset_dir / file) for file in files_to_tar]

    with tarfile.open(packaged_file_path, "w") as tar:
        for file in files_to_tar_as_paths:
            if file.exists():
                tar.add(file, arcname=file.name)
    for file in files_to_tar_as_paths:
        if file.exists():
            os.remove(file)
    shutil.rmtree(input_dataset_dir)
