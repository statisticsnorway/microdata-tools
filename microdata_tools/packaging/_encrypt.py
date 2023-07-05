import logging
import os
from pathlib import Path
import tarfile
import shutil

from cryptography.fernet import Fernet
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import padding

from microdata_tools.packaging.exceptions import ValidationException
from microdata_tools.packaging._utils import check_exists

logger = logging.getLogger()

CHUNK_SIZE_BYTES = 250_000_000  # 250 MB per chunk


def encrypt_dataset(
    rsa_keys_dir: Path,
    dataset_dir: Path,
    output_dir: Path,
) -> None:
    """
    Encrypts a dataset as follows:
        1. Generates the symmetric key for this dataset.
        2. Splits the dataset into chunks.
        3. Encrypts each chunk using the symmetric key.
        4. Encrypts the symmetric key using the RSA public key.
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

    csv_files = [
        file for file in dataset_dir.iterdir() if file.suffix == ".csv"
    ]

    csv_file = csv_files[0]
    dataset_name = csv_file.stem

    if dataset_name != dataset_dir.stem:
        raise ValidationException(
            f"The csv file name {dataset_name} should match "
            f"the dataset directory name {dataset_dir.stem}."
        )

    dataset_output_dir = output_dir / dataset_name
    os.makedirs(dataset_output_dir)
    os.makedirs(dataset_output_dir / "chunks", exist_ok=True)

    encrypted_symkey_file = dataset_output_dir / f"{dataset_name}.symkey.encr"

    # Generate and store symmetric key for this file
    symkey = Fernet.generate_key()
    fernet = Fernet(symkey)

    # Encrypt csv file
    chunk_count = 0
    logger.debug(f"Chunk size: {CHUNK_SIZE_BYTES} Bytes")

    with open(csv_file, "rb") as file:
        while True:
            data = file.read(CHUNK_SIZE_BYTES)
            if not data:
                break

            chunk_count += 1
            encrypted = fernet.encrypt(data)

            chunk_file = (
                dataset_output_dir / "chunks" / f"{chunk_count}.csv.encr"
            )
            with open(chunk_file, "wb") as chunk_output:
                chunk_output.write(encrypted)

    logger.debug(f"Csv file {csv_file} encrypted into {chunk_count} chunks")

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

    logger.debug(
        f"Key file for {csv_file} encrypted into {encrypted_symkey_file}"
    )


def _tar_encrypted_dataset(input_dir: Path, dataset_name: str) -> None:
    """
    Creates a tar file from encrypted dataset files.
    Removes the input directory after successful completion.
    :param input_dir: the input directory containing the dataset directory
    :param dataset_name: the name of the dataset
    """

    check_exists(input_dir)

    dataset_dir = input_dir / dataset_name

    if not dataset_dir.exists():
        raise ValidationException(f"Dataset directory {dataset_dir} not found")

    tar_file_name = f"{dataset_name}.tar"
    full_tar_file_name = input_dir / tar_file_name

    json_file = dataset_dir / f"{dataset_name}.json"
    if not json_file.exists():
        raise ValidationException(f"The required file {json_file} not found")

    files_to_tar = [dataset_dir / f"{dataset_name}.json"]
    chunk_dir = dataset_dir / "chunks"

    if chunk_dir.exists():
        chunk_files = [file for file in chunk_dir.iterdir()]
        if len(chunk_files) == 0:
            raise ValidationException(f"No files found in {chunk_dir}")

        md5_file = dataset_dir / f"{dataset_name}.md5"
        if not md5_file.exists():
            raise ValidationException(
                f"The required file {md5_file} is missing"
            )

        files_to_tar.extend(
            [
                dataset_dir / f"{dataset_name}.symkey.encr",
                dataset_dir / f"{dataset_name}.md5",
            ]
        )

    with tarfile.open(full_tar_file_name, "w") as tar:
        for file in files_to_tar:
            tar.add(file, arcname=os.path.basename(file))
        if chunk_dir.exists():
            tar.add(chunk_dir, arcname=os.path.basename(chunk_dir))

    shutil.rmtree(dataset_dir)

    logger.debug(f"Archive {full_tar_file_name} created")
