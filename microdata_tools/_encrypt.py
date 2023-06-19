import logging
import os
from pathlib import Path
import shutil
import tarfile

from cryptography.fernet import Fernet
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import padding

from microdata_tools.exceptions import ValidationException
from microdata_tools._utils import check_exists

logger = logging.getLogger()


def _encrypt_dataset(rsa_keys_dir: Path, dataset_dir: Path, output_dir: Path) -> None:
    """
    Encrypts a dataset as follows:
        1. Generates the symmetric key for this dataset.
        2. Encrypts the dataset using the symmetric key.
        3. Encrypts the symmetric key using the RSA public key.
    """

    check_exists(rsa_keys_dir)
    check_exists(dataset_dir)

    if not output_dir.exists():
        os.makedirs(output_dir)

    public_key_location = rsa_keys_dir / "microdata_public_key.pem"
    check_exists(public_key_location)

    # Read public key from file
    with open(public_key_location, "rb") as key_file:
        public_key = serialization.load_pem_public_key(
            key_file.read(), backend=default_backend()
        )

    csv_files = [file for file in dataset_dir.iterdir() if file.suffix == ".csv"]

    if len(csv_files) == 0:
        raise ValidationException(f"No csv files found in {dataset_dir}")

    if len(csv_files) > 1:
        raise ValidationException(f"There should only be one csv file in {dataset_dir}")

    csv_file = csv_files[0]
    dataset_name = csv_file.stem

    if dataset_name != dataset_dir.stem:
        raise ValidationException(
            f"The csv file name {dataset_name} should match "
            f"the dataset directory name {dataset_dir.stem}."
        )

    dataset_output_dir = output_dir / dataset_name
    os.makedirs(dataset_output_dir)

    encrypted_file = dataset_output_dir / f"{dataset_name}.csv.encr"
    encrypted_symkey_file = dataset_output_dir / f"{dataset_name}.symkey.encr"

    # Generate and store symmetric key for this file
    symkey = Fernet.generate_key()

    # Encrypt csv file
    with open(csv_file, "rb") as file:
        data = file.read()  # Read the bytes of the input file

    fernet = Fernet(symkey)
    encrypted = fernet.encrypt(data)

    with open(encrypted_file, "wb") as file:
        file.write(encrypted)

    logger.debug(f"Csv file {csv_file} encrypted into {encrypted_file}")

    encrypted_sym_key = public_key.encrypt(
        symkey,
        padding.OAEP(
            mgf=padding.MGF1(algorithm=hashes.SHA256()),
            algorithm=hashes.SHA256(),
            label=None,
        ),
    )

    # Store encrypted symkey to file
    with open(encrypted_symkey_file, "wb") as file:
        file.write(encrypted_sym_key)

    logger.debug(f"Key file for {csv_file} encrypted into {encrypted_symkey_file}")


def _tar_encrypted_dataset(input_dir: Path, dataset_name: str) -> None:
    """
    Creates a tar file from encrypted dataset files.
    Removes the input directory after successful completion.
    :param input_dir: the input directory containing the dataset directory
    :param dataset_name: the name of the dataset
    """

    check_exists(input_dir)

    if len(list((input_dir / dataset_name).iterdir())) == 0:
        raise ValidationException(f"No files found in {input_dir / dataset_name}")

    tar_file_name = f"{dataset_name}.tar"
    full_tar_file_name = input_dir / tar_file_name
    files_to_tar = [
        input_dir / dataset_name / f"{dataset_name}.csv.encr",
        input_dir / dataset_name / f"{dataset_name}.symkey.encr",
        input_dir / dataset_name / f"{dataset_name}.json",
    ]

    json_file = input_dir / dataset_name / f"{dataset_name}.json"
    if not json_file.exists():
        raise ValidationException(f"The required file {json_file} not found")

    with tarfile.open(full_tar_file_name, "w") as tar:
        for file in files_to_tar:
            if file.exists():
                logger.debug(f"Adding {file} to tar..")
                tar.add(file, arcname=file.name)

    shutil.rmtree(input_dir / dataset_name)
    logger.debug(f"Archive {full_tar_file_name} created")
