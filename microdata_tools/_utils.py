from pathlib import Path
import hashlib
from microdata_tools.exceptions import ValidationException


def check_exists(path: Path):
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
