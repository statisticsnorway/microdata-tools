import logging
import os
import shutil
from pathlib import Path

from microdata_tools._encrypt import _tar_encrypted_dataset, _encrypt_dataset

logger = logging.getLogger()


def package_dataset(
    rsa_keys_dir: Path,
    dataset_dir: Path,
    output_dir: Path,
) -> None:
    """
    Packages a dataset. It will encrypt and tar the dataset using
    the provided RSA public key. Only the CSV file will be encrypted.

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

    if len(csv_files) == 1:
        _encrypt_dataset(
            rsa_keys_dir=rsa_keys_dir,
            dataset_dir=dataset_dir,
            output_dir=output_dir,
        )
    else:
        if not dataset_output_dir.exists():
            os.makedirs(dataset_output_dir)

    shutil.copyfile(
        dataset_dir / f"{dataset_name}.json",
        dataset_output_dir / f"{dataset_name}.json",
    )
    _tar_encrypted_dataset(input_dir=output_dir, dataset_name=dataset_name)
