import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import x25519

from microdata_tools.packaging._utils import (
    load_hybrid_public_key,
)


def test_load_public_key_with_only_one_key_type_fails(tmp_path):
    """A PEM file with two X25519 public keys should be rejected."""
    key_path = tmp_path / "broken_public_key.pem"
    pem = (
        x25519.X25519PrivateKey.generate()
        .public_key()
        .public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo,
        )
    )
    key_path.write_bytes(pem + pem)

    with pytest.raises(TypeError) as e:
        load_hybrid_public_key(key_path)
    assert "must contain one ML-KEM-768 key and one X25519 key" in str(e.value)


def test_load_public_key_with_only_one_key_fails(tmp_path):
    """A PEM file with only one key should be rejected."""
    key_path = tmp_path / "broken_public_key.pem"
    pem = (
        x25519.X25519PrivateKey.generate()
        .public_key()
        .public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo,
        )
    )
    key_path.write_bytes(pem)

    with pytest.raises(ValueError) as e:
        load_hybrid_public_key(key_path)
    assert "Expected 2 PEM blocks in hybrid public key file" in str(e.value)
