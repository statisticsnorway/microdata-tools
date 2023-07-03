import logging
import os
from pathlib import Path
import shutil
import tarfile
from typing import List, Tuple

from cryptography.fernet import Fernet, InvalidToken
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

from microdata_tools.packaging.exceptions import (
    InvalidKeyError,
    InvalidTarFileContents,
)
from microdata_tools.packaging._utils import check_exists

logger = logging.getLogger()


def decrypt(rsa_keys_dir: Path, dataset_dir: Path, output_dir: Path):
    """
    Decrypts a dataset as follows:
        1. Decrypts the symmetric key using the RSA private key.
        2. Decrypts each chunk using the symmetric key.
        3. Merges the decrypted chunks into a single file.
    """

    dataset_name = dataset_dir.stem
    output_dataset_dir = output_dir / dataset_name
    chunk_dir = dataset_dir / "chunks"
    first_encrypted_chunk = Path(chunk_dir / "1.csv.encr")

    if not output_dataset_dir.exists():
        os.makedirs(output_dataset_dir)

    # Create temp directory for decrypted chunks
    decrypted_dir = output_dataset_dir / "decrypted"
    os.makedirs(decrypted_dir, exist_ok=True)

    if not output_dataset_dir.exists():
        os.makedirs(output_dataset_dir)

    if chunk_dir.exists() and first_encrypted_chunk.exists():
        logger.info(f"Encrypted file found in {dataset_dir}")

        with open(
            Path(rsa_keys_dir / "microdata_private_key.pem"), "rb"
        ) as key_file:
            private_key = serialization.load_pem_private_key(
                key_file.read(), password=None, backend=default_backend()
            )

        encrypted_symkey = Path(dataset_dir / f"{dataset_name}.symkey.encr")
        check_exists(encrypted_symkey)

        with open(encrypted_symkey, "rb") as f:
            symkey = f.read()

        decrypted_symkey = private_key.decrypt(
            symkey,
            padding.OAEP(
                mgf=padding.MGF1(algorithm=hashes.SHA256()),
                algorithm=hashes.SHA256(),
                label=None,
            ),
        )
        fernet = Fernet(decrypted_symkey)

        # Decrypt all the encrypted csv files in the directory
        for encrypted_file in chunk_dir.iterdir():
            if encrypted_file.name.endswith(".csv.encr"):
                csv_file = encrypted_file.stem

                # Decrypt csv file
                with open(encrypted_file, "rb") as file:
                    data = file.read()

                try:
                    decrypted_data = fernet.decrypt(data)

                    with open(decrypted_dir / f"{csv_file}", "wb") as file:
                        file.write(decrypted_data)

                except InvalidToken as exc:
                    raise InvalidKeyError(
                        f"Not able to decrypt {encrypted_file}, is symkey correct?"
                    ) from exc

                logger.debug(f"Decrypted {encrypted_file}")

        # Merges the decrypted csv files into a single file
        _combine_csv_files(decrypted_dir, dataset_dir / f"{dataset_name}.csv")

        _copy_decrypted_data_to_output_dir(
            dataset_dir, dataset_name, output_dataset_dir
        )

        # Remove decrypted chunks
        shutil.rmtree(decrypted_dir)

    _copy_metadata_file(dataset_dir, dataset_name, output_dataset_dir)


def _copy_decrypted_data_to_output_dir(
    dataset_dir, dataset_name, output_dataset_dir
):
    data_file_path = dataset_dir / f"{dataset_name}.csv"
    shutil.copy(
        data_file_path,
        output_dataset_dir / f"{dataset_name}.csv",
    )
    os.remove(data_file_path)


def _copy_metadata_file(dataset_dir, dataset_name, output_dataset_dir):
    metadata_file_path = dataset_dir / f"{dataset_name}.json"
    shutil.copy(
        metadata_file_path,
        output_dataset_dir / f"{dataset_name}.json",
    )
    os.remove(metadata_file_path)


def untar_encrypted_dataset(
    input_file: Path, dataset_name: str, untar_dir: Path
):
    with tarfile.open(input_file) as tar:
        _validate_tar_contents(tar.getnames(), dataset_name)
        tar.extractall(path=untar_dir)


def _validate_tar_contents(files: List[str], dataset_name: str) -> None:
    if f"{dataset_name}.json" not in files:
        raise InvalidTarFileContents(f"{dataset_name}.json not in .tar file")

    if len(files) > 1:
        if f"{dataset_name}.symkey.encr" not in files:
            raise InvalidTarFileContents(
                f"Tar file for {dataset_name} does not contain the required "
                f"{dataset_name}.symkey.encr file"
            )

        chunk_files = [str for str in files if str.endswith(".csv.encr")]

        if len(chunk_files) == 0:
            raise InvalidTarFileContents(
                f"Tar file for {dataset_name} does not contain any chunks files"
            )

        if f"{dataset_name}.md5" not in files:
            raise InvalidTarFileContents(
                f"Tar file for {dataset_name} does not contain the required "
                f"{dataset_name}.md5 file"
            )


def _combine_csv_files(input_dir: Path, output_file: Path) -> None:
    sorted_chunkpaths = _get_sorted_file_names(input_dir)
    logger.debug(
        f"\nCombining {len(sorted_chunkpaths)} files into {output_file}"
    )

    with open(output_file, "wb") as combined_file:
        for chunk_number, file_name in sorted_chunkpaths:
            with open(file_name, "rb") as chunk_file:
                chunk_data = chunk_file.read()
                combined_file.write(chunk_data)


# Turn files_names in input dir into a dictionary with the chunk number as key
# Then return the sorted dictionary
def _get_sorted_file_names(directory: Path) -> List[Tuple]:
    try:
        return sorted(
            [(int(f.stem), f) for f in directory.iterdir() if f.is_file()]
        )
    except ValueError as e:
        raise InvalidTarFileContents(
            "Failed to sort files in chunk directory "
        ) from e
