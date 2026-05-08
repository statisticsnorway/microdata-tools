import filecmp
import tarfile
from pathlib import Path

from pytest import MonkeyPatch, fail, raises

from microdata_tools import package_dataset, unpackage_dataset
from microdata_tools.packaging._decrypt import _validate_tar_contents
from microdata_tools.packaging.exceptions import (
    InvalidTarFileContents,
    UnpackagingError,
)

INPUT_DIRECTORY = Path("tests/resources/packaging/input_unpackage")
INPUT_PACKAGE_DIR = Path("tests/resources/packaging/input_package")


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


def test_validate_tar_contents_missing_kem():
    dataset_name = "DATASET_3"
    tarfile_path = INPUT_DIRECTORY / "MISSING_KEM.tar"

    tar = tarfile.open(tarfile_path)
    with raises(InvalidTarFileContents) as e:
        _validate_tar_contents(tar.getnames(), dataset_name)
    assert (
        str(e.value)
        == f"Tar file for {dataset_name} does not contain the required "
        f"{dataset_name}.kem.encr file"
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


def test_unpackage_dataset(mlkem_keys_dir, tmp_path):
    dataset_name = "VALID"
    dataset_dir = Path(f"{INPUT_PACKAGE_DIR}/{dataset_name}")
    package_output_dir = tmp_path / "packaged"
    unpackage_output_dir = tmp_path / "unpackaged"
    package_dataset(
        mlkem_keys_dir=mlkem_keys_dir,
        dataset_dir=dataset_dir,
        output_dir=package_output_dir,
    )
    tarfile_path = package_output_dir / f"{dataset_name}.tar"

    unpackage_dataset(tarfile_path, mlkem_keys_dir, unpackage_output_dir)

    output_dataset_dir = unpackage_output_dir / dataset_name
    assert Path(output_dataset_dir / f"{dataset_name}.json").exists()
    assert Path(output_dataset_dir / f"{dataset_name}.csv").exists()
    assert not Path(output_dataset_dir / "chunks/1.csv.encr").exists()
    assert not Path(output_dataset_dir / f"{dataset_name}.kem.encr").exists()
    assert Path(package_output_dir / f"{dataset_name}.tar").exists()
    assert not Path(package_output_dir / dataset_name).exists()

    actual = Path(output_dataset_dir / f"{dataset_name}.csv")
    expected = Path(dataset_dir / f"{dataset_name}.csv")
    assert filecmp.cmp(actual, expected)


def test_unpackage_dataset_multiple_chunks(
    monkeypatch: MonkeyPatch, mlkem_keys_dir, tmp_path
):
    dataset_name = "VALID"
    dataset_dir = Path(f"{INPUT_PACKAGE_DIR}/{dataset_name}")
    package_output_dir = tmp_path / "packaged"
    unpackage_output_dir = tmp_path / "unpackaged"
    # Produces more than 10 chunks
    monkeypatch.setattr(
        "microdata_tools.packaging._encrypt.CHUNK_SIZE_BYTES", 1
    )

    package_dataset(
        mlkem_keys_dir=mlkem_keys_dir,
        dataset_dir=dataset_dir,
        output_dir=package_output_dir,
    )

    result_file = package_output_dir / f"{dataset_name}.tar"
    assert result_file.exists()

    unpackage_dataset(result_file, mlkem_keys_dir, unpackage_output_dir)

    output_dataset_dir = unpackage_output_dir / dataset_name
    assert Path(output_dataset_dir / f"{dataset_name}.json").exists()
    assert Path(output_dataset_dir / f"{dataset_name}.csv").exists()
    assert not Path(output_dataset_dir / "chunks/1.csv.encr").exists()
    assert not Path(output_dataset_dir / "chunks/2.csv.encr").exists()
    assert not Path(output_dataset_dir / "chunks/3.csv.encr").exists()
    assert not Path(output_dataset_dir / f"{dataset_name}.kem.encr").exists()
    assert Path(package_output_dir / f"{dataset_name}.tar").exists()
    assert not Path(package_output_dir / dataset_name).exists()

    actual = Path(output_dataset_dir / f"{dataset_name}.csv")
    expected = Path(dataset_dir / f"{dataset_name}.csv")
    assert filecmp.cmp(actual, expected)


def test_unpackage_dataset_just_json(mlkem_keys_dir, output_dir):
    dataset_name = "ONLY_JSON"
    packaged_file_path = INPUT_DIRECTORY / f"{dataset_name}.tar"

    unpackage_dataset(packaged_file_path, mlkem_keys_dir, output_dir)

    output_dataset_dir = output_dir / dataset_name
    assert Path(output_dataset_dir / f"{dataset_name}.json").exists()
    assert not Path(output_dataset_dir / f"{dataset_name}.csv").exists()
    assert not Path(output_dataset_dir / f"{dataset_name}.csv.encr").exists()
    assert not Path(output_dataset_dir / f"{dataset_name}.kem.encr").exists()
    assert Path(INPUT_DIRECTORY / f"{dataset_name}.tar").exists()
    assert not Path(INPUT_DIRECTORY / dataset_name).exists()


def test_unpackage_dataset_failed(mlkem_keys_dir, output_dir):
    dataset_name = "MISSING_CHUNK"
    packaged_file_path = INPUT_DIRECTORY / f"{dataset_name}.tar"
    with raises(UnpackagingError):
        unpackage_dataset(packaged_file_path, mlkem_keys_dir, output_dir)

    assert Path(INPUT_DIRECTORY / f"{dataset_name}.tar").exists()
    assert not Path(INPUT_DIRECTORY / dataset_name).exists()
