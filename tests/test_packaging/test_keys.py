from pathlib import Path

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import x25519
from cryptography.hazmat.primitives.hpke import (
    MLKEM768X25519PrivateKey,
    MLKEM768X25519PublicKey,
)

from microdata_tools.packaging.keys import PrivateKey, PublicKey


def test_generate_and_serialize_keys(tmp_path: Path):
    private_key = PrivateKey.generate()
    private_key.write_to_file(tmp_path)
    private_key.public_key().write_to_file(tmp_path)

    loaded_private = PrivateKey.load_from_file(tmp_path / PrivateKey.FILENAME)
    loaded_public = PublicKey.load_from_file(tmp_path / PublicKey.FILENAME)

    assert isinstance(loaded_private.to_hpke_key(), MLKEM768X25519PrivateKey)
    assert isinstance(loaded_public.to_hpke_key(), MLKEM768X25519PublicKey)


def test_load_public_key_with_only_one_key_type_fails(tmp_path):
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

    with pytest.raises(ValueError) as e:
        PublicKey.load_from_file(key_path)
    assert "must contain one ML-KEM-768 key and one X25519 key" in str(e.value)


def test_load_public_key_with_only_one_key_fails(tmp_path):
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
        PublicKey.load_from_file(key_path)
    assert "Expected 2 PEM blocks in hybrid public key file" in str(e.value)
