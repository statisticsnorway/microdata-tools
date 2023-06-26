import logging
import os
import shutil
from pathlib import Path

from microdata_tools._encrypt import _tar_encrypted_dataset, encrypt_dataset
from microdata_tools.exceptions import ValidationException
from microdata_tools._utils import check_exists, write_checksum_to_file

logger = logging.getLogger()


def package_dataset(rsa_keys_dir: Path, dataset_dir: Path, output_dir: Path) -> None:
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
    csv_files = [file for file in dataset_dir.iterdir() if file.suffix == ".csv"]

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
