import logging
import os
import shutil
from pathlib import Path
from typing import Union

from microdata_tools.packaging._encrypt import (
    _tar_encrypted_dataset,
    encrypt_dataset,
)
from microdata_tools.packaging.exceptions import ValidationException
from microdata_tools.packaging._decrypt import decrypt, untar_encrypted_dataset
from microdata_tools.packaging._utils import (
    check_exists,
    write_checksum_to_file,
    compare_checksum_with_file,
    calculate_checksum,
)


logger = logging.getLogger()


def package_dataset(
    rsa_keys_dir: Path, dataset_dir: Path, output_dir: Path
) -> None:
    """
    Packages a dataset. It will encrypt and tar the dataset using
    the provided RSA public key. Only the CSV file will be encrypted.
    Creates a checksum file for the CSV file.

    :param rsa_keys_dir:
        directory containing public key file microdata_public_key.pem
    :param dataset_dir:
        directory containing the dataset files (CSV and JSON)
    :param output_dir:
        output directory
    :return:
        None
    """

    dataset_name = dataset_dir.stem
    dataset_output_dir = output_dir / dataset_name
    csv_files = [
        file for file in dataset_dir.iterdir() if file.suffix == ".csv"
    ]

    try:
        # check if json exists
        check_exists(dataset_dir / f"{dataset_name}.json")

        # Validate that there is only one csv file
        if len(csv_files) > 1:
            raise ValidationException(
                f"There should only be one csv file in {dataset_dir}"
            )

        if len(csv_files) == 1:
            write_checksum_to_file(csv_files[0])
            encrypt_dataset(
                rsa_keys_dir=rsa_keys_dir,
                dataset_dir=dataset_dir,
                output_dir=output_dir,
            )
        else:
            if not dataset_output_dir.exists():
                os.makedirs(dataset_output_dir)

        if Path(dataset_dir / f"{dataset_name}.md5").exists():
            shutil.copyfile(
                dataset_dir / f"{dataset_name}.md5",
                dataset_output_dir / f"{dataset_name}.md5",
            )

        shutil.copyfile(
            dataset_dir / f"{dataset_name}.json",
            dataset_output_dir / f"{dataset_name}.json",
        )
        _tar_encrypted_dataset(input_dir=output_dir, dataset_name=dataset_name)

    except Exception as exe:
        logger.error(f"Failed to package dataset {dataset_name}: {exe}")
        raise exe


def unpackage_dataset(
    packaged_file_path: Path,
    rsa_keys_dir: Path,
    output_dir: Path,
    archive_dir: Union[Path, None],
) -> None:
    """
    Unpackages a dataset. It will untar and decrypt the dataset using
    the provided RSA private key. Only the CSV file will be decrypted.
    Validates the checksum of the CSV file.

    :param packaged_file_path:
        a Path to the .tar file containing the dataset files
    :param rsa_keys_dir:
        directory containing the private key file microdata_private_key.pem
    :param output_dir:
        output directory
    :param archive_dir:
        optional archive directory where the .tar file will be moved
    :return:
        None
    """
    check_exists(packaged_file_path)
    check_exists(rsa_keys_dir)

    if not output_dir.exists():
        os.makedirs(output_dir)

    private_key_path = rsa_keys_dir / "microdata_private_key.pem"
    check_exists(private_key_path)

    dataset_name = packaged_file_path.stem
    dataset_dir = packaged_file_path.parent / dataset_name
    logger.info(f"Unpackaging {packaged_file_path}")

    try:
        untar_encrypted_dataset(packaged_file_path, dataset_name, dataset_dir)
        decrypt(rsa_keys_dir, dataset_dir, output_dir)
        _validate_csv_consistency(dataset_name, dataset_dir, output_dir)
        if archive_dir is not None:
            _archive(
                dataset_name, dataset_dir.parent, archive_dir, "unpackaged"
            )
        logger.info(f"Unpackaged {packaged_file_path}")
    except Exception as exc:
        if archive_dir is not None:
            _archive(dataset_name, dataset_dir.parent, archive_dir, "failed")
        logger.exception(f"Failed to unpackage {dataset_name}", exc_info=exc)


def _validate_csv_consistency(dataset_name, dataset_dir, output_dir):
    if Path(output_dir / dataset_name / f"{dataset_name}.csv").exists():
        calculated_checksum = calculate_checksum(
            output_dir / dataset_name / f"{dataset_name}.csv"
        )
        compare_checksum_with_file(
            dataset_dir / f"{dataset_name}.md5", calculated_checksum
        )


def _archive(
    dataset_name: str, input_dir: Path, archive_dir: Path, sub_dir: str
) -> None:
    archive_sub_dir = archive_dir / sub_dir

    if not archive_sub_dir.exists():
        os.makedirs(archive_sub_dir)

    shutil.move(
        input_dir / f"{dataset_name}.tar",
        archive_sub_dir / f"{dataset_name}.tar",
    )

    if Path(input_dir / dataset_name).exists():
        shutil.rmtree(input_dir / dataset_name)

    logger.debug(f"Archived files for {dataset_name} in {archive_sub_dir}")
