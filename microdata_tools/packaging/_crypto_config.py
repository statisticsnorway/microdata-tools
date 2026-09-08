from cryptography.hazmat.primitives.hpke import (
    AEAD,
    KDF,
    KEM,
    Suite,
)

NONCE_SIZE_BYTES = 12
HPKE_SUITE = Suite(KEM.MLKEM768_X25519, KDF.HKDF_SHA256, AEAD.AES_256_GCM)
HPKE_INFO = b"microdata-tools symmetric-key encryption"
