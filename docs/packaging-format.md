# The packaging format

This page describes what happens to the files when running `package_dataset()`.

## What happens when you package a dataset

1. A random symmetric encryption key is generated for the dataset.
2. The data file (CSV) is encrypted with this symmetric key, using AES-256-GCM, and stored as `<DATASET_NAME>.csv.encr`.
3. The symmetric key itself is then encrypted using HPKE, with the combined ML-KEM-768/X25519 public key supplied to you by microdata.no (`microdata_public_key.pem`). The resulting HPKE ciphertext is stored as `<DATASET_NAME>.kem.encr`.
4. The encrypted data file, the encrypted key, and your metadata file (`<DATASET_NAME>.json`, which is **not** encrypted) are gathered into a single `<DATASET_NAME>.tar` archive.

The combination of AES-256-GCM for the bulk data and HPKE/ML-KEM-768/X25519 (a post-quantum-safe key encapsulation scheme combined with X25519) for the key means that only the holder of the corresponding private key — microdata.no — can ever recover the symmetric key needed to decrypt the data.

!!! note "Getting your public key"
    Your public key is supplied through datastore-admin once you're logged in (look for a "copy public key" button). Contact the microdata.no team if you can't find it.


## Archive contents

A packaged `<DATASET_NAME>.tar` archive contains:

| File | Required | Description |
|---|---|---|
| `<DATASET_NAME>.json` | Yes | The metadata file, unencrypted. |
| `<DATASET_NAME>.csv.encr` | Only if a data file was provided | The encrypted data file. |
| `<DATASET_NAME>.kem.encr` | Required if `.csv.encr` is present | The HPKE-encrypted symmetric key needed to decrypt the data file. |

## Metadata-only datasets

If a dataset has no data file (see [Getting started](index.md)), the resulting archive will only contain the metadata JSON file — there is nothing to encrypt.
