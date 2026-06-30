import logging
import os
import shutil
import tarfile
from pathlib import Path
from typing import List, Tuple

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.hpke import (
    MLKEM768X25519PrivateKey,
)

from microdata_tools.packaging._crypto_config import (
    HPKE_INFO,
    HPKE_SUITE,
    NONCE_SIZE_BYTES,
)
from microdata_tools.packaging._utils import (
    check_exists,
)
from microdata_tools.packaging.exceptions import (
    InvalidKeyError,
    InvalidTarFileContents,
)
from microdata_tools.packaging.keys import PrivateKey

logger = logging.getLogger()


def decrypt(private_key_dir: Path, dataset_dir: Path, output_dir: Path) -> None:
    """
    Decrypts a dataset as follows:
        1. Recovers the symmetric key using the hybrid private key
           (HPKE with ML-KEM-768 + X25519).
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

    if chunk_dir.exists() and first_encrypted_chunk.exists():
        logger.info(f"Encrypted file found in {dataset_dir}")

        # Read private key from file
        private_key = PrivateKey.load_from_file(
            Path(private_key_dir / "microdata_private_key.pem")
        ).to_hpke_key()

        if not isinstance(private_key, MLKEM768X25519PrivateKey):
            raise TypeError(
                "Private key is not a hybrid ML-KEM-768-X25519 private key."
            )
        # Read the hpke ciphertext from file and recover the symmetric key
        hpke_ciphertext_path = Path(dataset_dir / f"{dataset_name}.kem.encr")
        check_exists(hpke_ciphertext_path)

        with open(hpke_ciphertext_path, "rb") as f:
            hpke_ciphertext = f.read()

        try:
            decrypted_symkey = HPKE_SUITE.decrypt(
                hpke_ciphertext, private_key, info=HPKE_INFO
            )
        except Exception as e:
            raise InvalidKeyError(
                "Unable to recover symmetric key. Is the private key correct?"
            ) from e

        aesgcm = AESGCM(decrypted_symkey)

        # Decrypt all the encrypted csv files in the directory
        for encrypted_file in chunk_dir.iterdir():
            if encrypted_file.name.endswith(".csv.encr"):
                csv_file = encrypted_file.stem

                # Decrypt csv file
                with open(encrypted_file, "rb") as file:
                    data = file.read()
                    nonce, ciphertext = (
                        data[:NONCE_SIZE_BYTES],
                        data[NONCE_SIZE_BYTES:],
                    )

                try:
                    decrypted_data = aesgcm.decrypt(nonce, ciphertext, None)

                    with open(decrypted_dir / f"{csv_file}", "wb") as file:
                        file.write(decrypted_data)

                except InvalidTag as exc:
                    raise InvalidKeyError(
                        f"Not able to decrypt {encrypted_file}, "
                        f"is symkey correct?"
                    ) from exc

                logger.debug(f"Decrypted {encrypted_file}")

        del decrypted_symkey
        del aesgcm

        # Merges the decrypted csv files into a single file
        _combine_csv_files(decrypted_dir, dataset_dir / f"{dataset_name}.csv")

        _copy_decrypted_data_to_output_dir(
            dataset_dir, dataset_name, output_dataset_dir
        )

        # Remove decrypted chunks
        shutil.rmtree(decrypted_dir)

    _copy_metadata_file(dataset_dir, dataset_name, output_dataset_dir)


def _copy_decrypted_data_to_output_dir(
    dataset_dir: Path, dataset_name: str, output_dataset_dir: Path
) -> None:
    data_file_path = dataset_dir / f"{dataset_name}.csv"
    shutil.copy(
        data_file_path,
        output_dataset_dir / f"{dataset_name}.csv",
    )
    os.remove(data_file_path)


def _copy_metadata_file(
    dataset_dir: Path, dataset_name: str, output_dataset_dir: Path
) -> None:
    metadata_file_path = dataset_dir / f"{dataset_name}.json"
    shutil.copy(
        metadata_file_path,
        output_dataset_dir / f"{dataset_name}.json",
    )
    os.remove(metadata_file_path)


def untar_encrypted_dataset(
    input_file: Path, dataset_name: str, untar_dir: Path
) -> None:
    with tarfile.open(input_file) as tar:
        _validate_tar_contents(tar.getnames(), dataset_name)
        tar.extractall(path=untar_dir)


def _validate_tar_contents(files: List[str], dataset_name: str) -> None:
    filenames = {os.path.basename(f) for f in files}
    if f"{dataset_name}.json" not in filenames:
        raise InvalidTarFileContents(f"{dataset_name}.json not in .tar file")

    if len(files) > 1:
        if f"{dataset_name}.kem.encr" not in filenames:
            raise InvalidTarFileContents(
                f"Tar file for {dataset_name} does not contain the required "
                f"{dataset_name}.kem.encr file"
            )

        chunk_files = [str for str in files if str.endswith(".csv.encr")]

        if len(chunk_files) == 0:
            raise InvalidTarFileContents(
                f"Tar file for {dataset_name} does not contain any chunks files"
            )

        if f"{dataset_name}.md5" not in filenames:
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
def _get_sorted_file_names(directory: Path) -> List[Tuple[int, Path]]:
    try:
        return sorted(
            [(int(f.stem), f) for f in directory.iterdir() if f.is_file()]
        )
    except ValueError as e:
        raise InvalidTarFileContents(
            "Failed to sort files in chunk directory "
        ) from e
