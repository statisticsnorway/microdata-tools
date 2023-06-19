import logging
import os
from pathlib import Path
import shutil
import tarfile
from typing import List

from cryptography.fernet import Fernet, InvalidToken
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

from microdata_tools.exceptions import InvalidKeyError, InvalidTarFileContents
from microdata_tools._utils import check_exists

logger = logging.getLogger()


def decrypt(rsa_keys_dir: Path, dataset_dir: Path, output_dir: Path):
    dataset_name = dataset_dir.stem
    encrypted_csv_file = Path(dataset_dir / f"{dataset_name}.csv.encr")
    output_dataset_dir = output_dir / dataset_name

    if not output_dataset_dir.exists():
        os.makedirs(output_dataset_dir)

    if encrypted_csv_file.exists():
        logger.info(f"Encrypted file found in {dataset_dir}")

        with open(Path(rsa_keys_dir / "microdata_private_key.pem"), "rb") as key_file:
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

        with open(encrypted_csv_file, "rb") as f:
            data = f.read()

        fernet = Fernet(decrypted_symkey)
        try:
            decrypted = fernet.decrypt(data)
            with open(Path(dataset_dir / f"{dataset_name}.csv"), "wb") as f:
                f.write(decrypted)
        except InvalidToken as exc:
            raise InvalidKeyError(
                f"Not able to decrypt {encrypted_csv_file}, is symkey correct?"
            ) from exc

        logger.debug(f"Decrypted {encrypted_csv_file}")
        _copy_decrypted_data_to_output_dir(
            dataset_dir, dataset_name, output_dataset_dir
        )

    _copy_metadata_file(dataset_dir, dataset_name, output_dataset_dir)


def _copy_decrypted_data_to_output_dir(dataset_dir, dataset_name, output_dataset_dir):
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


def untar_encrypted_dataset(input_file: Path, dataset_name: str, untar_dir: Path):
    with tarfile.open(input_file) as tar:
        _validate_tar_contents(tar.getnames(), dataset_name)
        tar.extractall(path=untar_dir)


def _validate_tar_contents(files: List[str], dataset_name: str) -> None:
    if len(files) == 1:
        if f"{dataset_name}.json" not in files:
            raise InvalidTarFileContents(f"{dataset_name}.json not in .tar file")
        else:
            return

    if len(files) == 3:
        if (
            f"{dataset_name}.json" not in files
            or f"{dataset_name}.csv.encr" not in files
            or f"{dataset_name}.symkey.encr" not in files
        ):
            raise InvalidTarFileContents(
                f"Tar file for {dataset_name} does not contain the "
                f"required {dataset_name}.csv.encr "
                f"or {dataset_name}.symkey.encr file "
                f"or {dataset_name}.json file "
            )
    else:
        raise InvalidTarFileContents(
            f"Tar file for {dataset_name} contains "
            f"incorrect number ({len(files)}) of files: {files}"
        )
