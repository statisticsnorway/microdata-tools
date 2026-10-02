# microdata-tools
Tools for the [microdata.no](https://www.microdata.no/) platform

`microdata-tools` is a Python library with two audiences:
- **Data owners** preparing datasets for microdata.no: validate your data and metadata, then encrypt and package the files into a `.tar` archive ready for upload.
- **microdata.no services** (such as job-executor) that receive packaged datasets: unpackage and decrypt them, and validate them on the receiving end.

A dataset normally contains:
- A CSV file with one identifier, one value and the dates that describe when the value applies.
- A JSON file describing the dataset, its variable and its values.

## Installation
`microdata-tools` can be installed from PyPI using pip:
```
pip install microdata-tools
```

## Documentation

If you're a data owner preparing a dataset, the full guide lives on the documentation site:

- **[Getting started](https://statisticsnorway.github.io/microdata-tools/)** — install, prepare, validate and package your dataset, step by step.
- **[The metadata model](https://statisticsnorway.github.io/microdata-tools/metadata-model/)** — a field-by-field reference for the metadata JSON file.
- **[The packaging format](https://statisticsnorway.github.io/microdata-tools/packaging-format/)** — how your data is encrypted and packaged.

The sections below are aimed at developers integrating `microdata-tools` into another service (such as job-executor), and at contributors to this repository.

## Unpackaging a dataset

The `unpackage_dataset()` function will reverse the packaging step: it untars the archive and use the combined ML-KEM-768/X25519 private key from `microdata_private_key.pem` to recover the symmetric key, which is then used to decrypt the data file.

The packaged file must have the `<DATASET_NAME>.tar` extension. Its contents should be:

- `<DATASET_NAME>.json`: Required metadata file.
- `<DATASET_NAME>.csv.encr`: Optional encrypted data file.
- `<DATASET_NAME>.kem.encr`: Optional HPKE ciphertext file containing the encrypted symmetric key. Required if `.csv.encr` is present.

Decryption uses the combined ML-KEM-768/X25519 private key located at `PRIVATE_KEY_DIR` to recover the symmetric decryption key. The dataset is then stored in `output_dir/archive/unpackaged` after a successful run, or `output_dir/archive/failed` after an unsuccessful one.
For details on how packaging encrypts a dataset in the first place, see [the packaging format](https://statisticsnorway.github.io/microdata-tools/packaging-format/).


## Contribute

### Set up
To work on this repository you need to install [uv](https://docs.astral.sh/uv/):
```
# macOS / linux / BashOnWindows
curl -LsSf https://astral.sh/uv/install.sh | sh

# Windows powershell
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```
Then install the virtual environment from the root directory:
```
uv sync
```

### Running unit tests
Open terminal and go to root directory of the project and run:
````
uv run pytest
````

### Pre-commit
There are currently 3 active rules: Ruff-format, Ruff-lint and sync lock file.
Install pre-commit 
```sh
pip install pre-commit
```
If you've made changes to the pre-commit-config.yaml or its a new project install the hooks with:
```sh
pre-commit install
```
Now it should run when you do:
```sh
git commit
```

By default it only runs against changed files. To force the hooks to run against all files:
```sh
pre-commit run --all-files
```
if you dont have it installed on your system you can use: 
(but then it won't run when you use the git-cli)
```sh
uv run pre-commit
```
Read more about [pre-commit](https://pre-commit.com/#intro)
