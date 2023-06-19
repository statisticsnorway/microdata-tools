import logging
import os
from pathlib import Path
import shutil
from typing import Union
from microdata_tools._utils import check_exists
from microdata_tools._decrypt import decrypt, untar_encrypted_dataset

logger = logging.getLogger()


def unpackage_dataset(
    packaged_file_path: Path,
    rsa_keys_dir: Path,
    output_dir: Path,
    archive_dir: Union[Path, None],
) -> None:
    """
    Unpackages a dataset. It will untar and decrypt the dataset using
    the provided RSA private key. Only the CSV file will be decrypted.

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
        if archive_dir is not None:
            _archive(dataset_name, dataset_dir.parent, archive_dir, "unpackaged")
        logger.info(f"Unpackaged {packaged_file_path}")
    except Exception as exc:
        if archive_dir is not None:
            _archive(dataset_name, dataset_dir.parent, archive_dir, "failed")
        logger.exception(f"Failed to unpackage {dataset_name}", exc_info=exc)


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
