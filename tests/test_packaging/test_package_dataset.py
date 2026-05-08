import tarfile
from pathlib import Path

from pytest import MonkeyPatch

from microdata_tools import package_dataset

INPUT_DIRECTORY = Path("tests/resources/packaging/input_package")


def test_package_dataset(mlkem_keys_dir, output_dir):
    dataset_name = "VALID"

    package_dataset(
        mlkem_keys_dir=mlkem_keys_dir,
        dataset_dir=Path(f"{INPUT_DIRECTORY}/{dataset_name}"),
        output_dir=output_dir,
    )

    result_file = output_dir / f"{dataset_name}.tar"
    assert result_file.exists()

    assert not Path(output_dir / f"{dataset_name}").exists()
    assert not Path(
        INPUT_DIRECTORY / f"{dataset_name}" / f"{dataset_name}.md5"
    ).exists()

    with tarfile.open(result_file, "r:") as tar:
        tarred_files = [file.name for file in tar.getmembers()]
        assert (
            len(tarred_files) == 5
        )  # the chunk dir adds an extra "file" when peeking
        assert "chunks/1.csv.encr" in tarred_files
        assert f"{dataset_name}.kem.encr" in tarred_files
        assert f"{dataset_name}.json" in tarred_files
        assert f"{dataset_name}.md5" in tarred_files


def test_package_dataset_multiple_chunks(
    monkeypatch: MonkeyPatch, mlkem_keys_dir, output_dir
):
    dataset_name = "VALID"

    monkeypatch.setattr(
        "microdata_tools.packaging._encrypt.CHUNK_SIZE_BYTES", 5
    )

    package_dataset(
        mlkem_keys_dir=mlkem_keys_dir,
        dataset_dir=Path(f"{INPUT_DIRECTORY}/{dataset_name}"),
        output_dir=output_dir,
    )

    result_file = output_dir / f"{dataset_name}.tar"
    assert result_file.exists()

    assert not Path(output_dir / f"{dataset_name}").exists()
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
        assert f"{dataset_name}.kem.encr" in tarred_files
        assert f"{dataset_name}.json" in tarred_files
        assert f"{dataset_name}.md5" in tarred_files


def test_package_dataset_just_json(mlkem_keys_dir, output_dir):
    dataset_name = "ONLY_JSON"

    package_dataset(
        mlkem_keys_dir=mlkem_keys_dir,
        dataset_dir=Path(f"{INPUT_DIRECTORY}/{dataset_name}"),
        output_dir=output_dir,
    )

    result_file = output_dir / f"{dataset_name}.tar"
    assert result_file.exists()

    assert not Path(output_dir / f"{dataset_name}").exists()

    with tarfile.open(result_file, "r:") as tar:
        tarred_files = [file.name for file in tar.getmembers()]
        assert len(tarred_files) == 1
        assert f"{dataset_name}.json" in tarred_files
