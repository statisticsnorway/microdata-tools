# microdata-tools
Tools for the microdata.no platform

## Installation
`microdata-tools` can be installed from PyPI using pip:
```
pip install microdata-tools
```

## Usage
Once you have your metadata and data files ready to go, they should be named and stored like this:
```
my-input-directory/
    MY_DATASET_NAME/
        MY_DATASET_NAME.csv
        MY_DATASET_NAME.json
```
The CSV file is optional in some cases.

### Package dataset
The `package_dataset()` function will encrypt and package your datset as a tar archive. The process is as follows:

1. Generate the symmetric key for a dataset.
2. Encrypt the dataset data (CSV) using the symmetric key and store the encrypted file as `<DATASET_NAME>.csv.encr`
3. Encrypt the symmetric key using the asymmetric rsa public key and store the encrypted file as `<DATASET_NAME>.symkey.encr`
4. Gather the encrypted CSV, encrypted symmetric key and metadata (JSON) file in one tar file.

## Example
Python script that uses a public RSA key named `microdata_public_key.pem` and packages a dataset:
```py
from pathlib import Path
from microdata_tools import package_dataset

RSA_KEYS_DIRECTORY = Path("tests/resources/rsa_keys")
DATASET_DIRECTORY = Path("tests/resources/input/DATASET_1")
OUTPUT_DIRECTORY = Path("tests/resources/output")

package_dataset(
    rsa_keys_dir=RSA_KEYS_DIRECTORY,
    dataset_dir=DATASET_DIRECTORY,
    output_dir=OUTPUT_DIRECTORY,
)
```
