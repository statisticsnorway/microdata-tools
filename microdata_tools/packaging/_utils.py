import hashlib
import re
from pathlib import Path

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import mlkem, x25519
from cryptography.hazmat.primitives.hpke import (
    MLKEM768X25519PrivateKey,
    MLKEM768X25519PublicKey,
)

from microdata_tools.packaging.exceptions import (
    CsvConsistencyException,
    ValidationException,
)


def check_exists(path: Path) -> None:
    if not path.exists():
        raise ValidationException(f"The path {path} does not exist")


def calculate_checksum(csv_file: Path) -> str:
    """
    Reads a file in chunks and returns the MD5 hash of the file
    """
    CHUNK_SIZE = 32768
    hash_md5 = hashlib.md5()
    with open(csv_file, "rb") as f:
        for chunk in iter(lambda: f.read(CHUNK_SIZE), b""):
            hash_md5.update(chunk)

    return hash_md5.hexdigest()


def write_checksum_to_file(csv_file: Path) -> None:
    hash_file = str(csv_file).replace(".csv", ".md5")
    with open(hash_file, "w") as file:
        file.write(calculate_checksum(csv_file))


def compare_checksum_with_file(
    md5_file: Path, calculated_checksum: str
) -> None:
    with open(md5_file, "r") as file:
        checksum_from_file = file.readlines()[0]
        if calculated_checksum != checksum_from_file.strip():
            raise CsvConsistencyException(
                "MD5 checksums do not match. The csv file may be corrupted!"
            )


def load_hybrid_public_key(
    public_key_location: Path,
) -> MLKEM768X25519PublicKey:
    """
    Loads a PEM file with two public key blocks (one ML-KEM-768, one X25519,
    in any order) and combines them into a hybrid key for use with HPKE.
    """
    PEM_PUBLIC_BLOCK = re.compile(
        rb"-----BEGIN PUBLIC KEY-----.*?-----END PUBLIC KEY-----",
        re.DOTALL,
    )
    pem_data = public_key_location.read_bytes()
    blocks = PEM_PUBLIC_BLOCK.findall(pem_data)

    if len(blocks) != 2:
        raise ValueError(
            "Expected 2 PEM blocks in hybrid public key file, "
            f"found {len(blocks)}"
        )
    parsed_keys = [serialization.load_pem_public_key(b) for b in blocks]

    mlkem_key = next(
        (
            key
            for key in parsed_keys
            if isinstance(key, mlkem.MLKEM768PublicKey)
        ),
        None,
    )
    x25519_key = next(
        (key for key in parsed_keys if isinstance(key, x25519.X25519PublicKey)),
        None,
    )

    if mlkem_key is None or x25519_key is None:
        raise TypeError(
            "Hybrid public key file must contain one ML-KEM-768 key "
            "and one X25519 key."
        )

    return MLKEM768X25519PublicKey(mlkem_key=mlkem_key, x25519_key=x25519_key)


def load_hybrid_private_key(
    private_key_location: Path,
) -> MLKEM768X25519PrivateKey:
    """
    Loads a PEM file with two private key blocks (one ML-KEM-768, one X25519,
    in any order) and combines them into a hybrid key for use with HPKE.
    """
    PEM_PRIVATE_BLOCK = re.compile(
        rb"-----BEGIN PRIVATE KEY-----.*?-----END PRIVATE KEY-----",
        re.DOTALL,
    )
    pem_data = private_key_location.read_bytes()
    blocks = PEM_PRIVATE_BLOCK.findall(pem_data)

    if len(blocks) != 2:
        raise ValueError(
            "Expected 2 PEM blocks in hybrid private key file, "
            f"found {len(blocks)}"
        )
    parsed_keys = [
        serialization.load_pem_private_key((b), password=None) for b in blocks
    ]

    mlkem_key = next(
        (
            key
            for key in parsed_keys
            if isinstance(key, mlkem.MLKEM768PrivateKey)
        ),
        None,
    )
    x25519_key = next(
        (
            key
            for key in parsed_keys
            if isinstance(key, x25519.X25519PrivateKey)
        ),
        None,
    )

    if mlkem_key is None or x25519_key is None:
        raise TypeError(
            "Hybrid private key file must contain one ML-KEM-768 key "
            "and one X25519 key."
        )

    return MLKEM768X25519PrivateKey(mlkem_key=mlkem_key, x25519_key=x25519_key)
