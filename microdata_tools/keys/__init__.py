import re
from pathlib import Path

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import mlkem, x25519
from cryptography.hazmat.primitives.hpke import (
    MLKEM768X25519PrivateKey,
    MLKEM768X25519PublicKey,
)

PEM_PRIVATE_BLOCK = re.compile(
    rb"-----BEGIN PRIVATE KEY-----.*?-----END PRIVATE KEY-----",
    re.DOTALL,
)

PEM_PUBLIC_BLOCK = re.compile(
    rb"-----BEGIN PUBLIC KEY-----.*?-----END PUBLIC KEY-----",
    re.DOTALL,
)


class PrivateKey:
    """Hybrid ML-KEM-768 + X25519 private key for use with HPKE."""

    FILENAME = "microdata_private_key.pem"

    def __init__(
        self,
        mlkem_key: mlkem.MLKEM768PrivateKey,
        x25519_key: x25519.X25519PrivateKey,
    ) -> None:
        self.mlkem_key = mlkem_key
        self.x25519_key = x25519_key

    @classmethod
    def generate(cls) -> "PrivateKey":
        """Generates a new hybrid private key."""
        return cls(
            mlkem_key=mlkem.MLKEM768PrivateKey.generate(),
            x25519_key=x25519.X25519PrivateKey.generate(),
        )

    @classmethod
    def load_from_file(cls, path: Path) -> "PrivateKey":
        """Loads a hybrid private key from a PEM file containing one
        ML-KEM-768 and one X25519 PRIVATE KEY block, in any order."""
        blocks = PEM_PRIVATE_BLOCK.findall(path.read_bytes())
        if len(blocks) != 2:
            raise ValueError(
                f"Expected 2 PEM blocks in hybrid private key file, "
                f"found {len(blocks)}"
            )

        parsed_keys = [
            serialization.load_pem_private_key(block, password=None)
            for block in blocks
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
            raise ValueError(
                "Hybrid private key file must contain one ML-KEM-768 key "
                "and one X25519 key."
            )

        return cls(mlkem_key=mlkem_key, x25519_key=x25519_key)

    def public_key(self) -> "PublicKey":
        """Returns the matching hybrid PublicKey."""
        return PublicKey(
            mlkem_key=self.mlkem_key.public_key(),
            x25519_key=self.x25519_key.public_key(),
        )

    def serialize(self) -> bytes:
        """Serializes both halves as concatenated PKCS8 PEM blocks."""
        return self.mlkem_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        ) + self.x25519_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )

    def write_to_file(self, target_dir: Path) -> None:
        """Writes the serialized key to
        ``<target_dir>/<PrivateKey.FILENAME>``."""
        (target_dir / self.FILENAME).write_bytes(self.serialize())

    def to_hpke_key(self) -> MLKEM768X25519PrivateKey:
        """Returns the combined HPKE-usable key object."""
        return MLKEM768X25519PrivateKey(
            mlkem_key=self.mlkem_key,
            x25519_key=self.x25519_key,
        )


class PublicKey:
    """Hybrid ML-KEM-768 + X25519 public key for use with HPKE."""

    FILENAME = "microdata_public_key.pem"

    def __init__(
        self,
        mlkem_key: mlkem.MLKEM768PublicKey,
        x25519_key: x25519.X25519PublicKey,
    ) -> None:
        self.mlkem_key = mlkem_key
        self.x25519_key = x25519_key

    @classmethod
    def load_from_file(cls, path: Path) -> "PublicKey":
        """Loads a hybrid public key from a PEM file containing one
        ML-KEM-768 and one X25519 PUBLIC KEY block, in any order."""
        blocks = PEM_PUBLIC_BLOCK.findall(path.read_bytes())
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
            (
                key
                for key in parsed_keys
                if isinstance(key, x25519.X25519PublicKey)
            ),
            None,
        )

        if mlkem_key is None or x25519_key is None:
            raise ValueError(
                "Hybrid public key file must contain one ML-KEM-768 key "
                "and one X25519 key."
            )

        return cls(mlkem_key=mlkem_key, x25519_key=x25519_key)

    def serialize(self) -> bytes:
        """Serializes both keys as concatenated PEM blocks."""
        return self.mlkem_key.public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo,
        ) + self.x25519_key.public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo,
        )

    def write_to_file(self, target_dir: Path) -> None:
        """Writes the serialized keys to
        ``<target_dir>/<PublicKey.FILENAME>``."""
        (target_dir / self.FILENAME).write_bytes(self.serialize())

    def to_hpke_key(self) -> MLKEM768X25519PublicKey:
        """Returns the combined HPKE-usable key object."""
        return MLKEM768X25519PublicKey(
            mlkem_key=self.mlkem_key,
            x25519_key=self.x25519_key,
        )
