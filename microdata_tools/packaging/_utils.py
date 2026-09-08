import hashlib
from pathlib import Path

from microdata_tools.packaging.exceptions import (
    CsvConsistencyException,
    ValidationException,
)


def check_exists(path: Path) -> None:
    if not path.exists():
        raise ValidationException(f"The path {path} does not exist")


def calculate_checksum(csv_file: Path) -> str:
    """
    Reads a file in chunks and returns the SHA-256 hash of the file
    """
    CHUNK_SIZE = 32768
    hash_sha256 = hashlib.sha256()
    with open(csv_file, "rb") as f:
        for chunk in iter(lambda: f.read(CHUNK_SIZE), b""):
            hash_sha256.update(chunk)

    return hash_sha256.hexdigest()


def write_checksum_to_file(csv_file: Path) -> None:
    hash_file = str(csv_file).replace(".csv", ".sha256")
    with open(hash_file, "w") as file:
        file.write(calculate_checksum(csv_file))


def compare_checksum_with_file(
    sha256_file: Path, calculated_checksum: str
) -> None:
    with open(sha256_file, "r") as file:
        checksum_from_file = file.readlines()[0]
        if calculated_checksum != checksum_from_file.strip():
            raise CsvConsistencyException(
                "SHA-256 checksums do not match. The csv file may be corrupted!"
            )
