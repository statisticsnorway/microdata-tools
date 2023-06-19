import filecmp
import os
import shutil
import tarfile
from pathlib import Path
from typing import List

from microdata_tools import unpackage_dataset


RSA_KEYS_DIRECTORY = Path("tests/resources/rsa_test_key")
INPUT_DIRECTORY = Path("tests/resources/input_unpackage")
OUTPUT_DIRECTORY = Path("tests/resources/output")
ARCHIVE_DIR = Path("tests/resources/archive")


def setup_function():
    shutil.copytree("tests/resources", "tests/resources_backup")


def teardown_function():
    shutil.rmtree("tests/resources")
    shutil.move("tests/resources_backup", "tests/resources")


def test_unpackage_dataset():
    dataset_name = "DATASET_1"
    packaged_file_path = INPUT_DIRECTORY / "DATASET_1.tar"
    input_dataset_dir = INPUT_DIRECTORY / dataset_name
    _tar_files(
        packaged_file_path,
        input_dataset_dir,
        [
            f"{dataset_name}.csv.encr",
            f"{dataset_name}.symkey.encr",
            f"{dataset_name}.json",
        ],
    )

    unpackage_dataset(
        packaged_file_path, RSA_KEYS_DIRECTORY, OUTPUT_DIRECTORY, ARCHIVE_DIR
    )

    output_dataset_dir = OUTPUT_DIRECTORY / dataset_name
    assert Path(output_dataset_dir / f"{dataset_name}.json").exists()
    assert Path(output_dataset_dir / f"{dataset_name}.csv").exists()
    assert not Path(output_dataset_dir / f"{dataset_name}.csv.encr").exists()
    assert not Path(output_dataset_dir / f"{dataset_name}.symkey.encr").exists()
    assert not Path(INPUT_DIRECTORY / "DATASET_1.tar").exists()
    assert not Path(INPUT_DIRECTORY / dataset_name).exists()
    assert Path(ARCHIVE_DIR / "unpackaged" / "DATASET_1.tar").exists()

    actual = Path(output_dataset_dir / f"{dataset_name}.csv")
    expected = Path("tests/resources/expected_unpackage/DATASET_1_expected.csv")
    assert filecmp.cmp(actual, expected)


def test_unpackage_dataset_just_json():
    dataset_name = "DATASET_2"
    packaged_file_path = INPUT_DIRECTORY / "DATASET_2.tar"
    input_dataset_dir = INPUT_DIRECTORY / dataset_name
    _tar_files(
        packaged_file_path,
        input_dataset_dir,
        [
            f"{dataset_name}.json",
        ],
    )

    unpackage_dataset(
        packaged_file_path, RSA_KEYS_DIRECTORY, OUTPUT_DIRECTORY, ARCHIVE_DIR
    )

    output_dataset_dir = OUTPUT_DIRECTORY / dataset_name
    assert Path(output_dataset_dir / f"{dataset_name}.json").exists()
    assert not Path(output_dataset_dir / f"{dataset_name}.csv").exists()
    assert not Path(output_dataset_dir / f"{dataset_name}.csv.encr").exists()
    assert not Path(output_dataset_dir / f"{dataset_name}.symkey.encr").exists()
    assert not Path(INPUT_DIRECTORY / "DATASET_2.tar").exists()
    assert not Path(INPUT_DIRECTORY / dataset_name).exists()
    assert Path(ARCHIVE_DIR / "unpackaged" / "DATASET_2.tar").exists()


def test_unpackage_dataset_failed():
    dataset_name = "DATASET_1"
    packaged_file_path = INPUT_DIRECTORY / "DATASET_1.tar"
    input_dataset_dir = INPUT_DIRECTORY / dataset_name
    _tar_files(
        packaged_file_path,
        input_dataset_dir,
        [
            f"{dataset_name}.symkey.encr",
            f"{dataset_name}.json",
        ],
    )

    unpackage_dataset(
        packaged_file_path, RSA_KEYS_DIRECTORY, OUTPUT_DIRECTORY, ARCHIVE_DIR
    )

    assert not Path(INPUT_DIRECTORY / "DATASET_1.tar").exists()
    assert not Path(INPUT_DIRECTORY / dataset_name).exists()
    assert Path(ARCHIVE_DIR / "failed" / "DATASET_1.tar").exists()


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
