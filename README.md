# microdata-tools
Tools for the [microdata.no](https://www.microdata.no/) platform

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
The `package_dataset()` function will encrypt and package your dataset as a tar archive. The process is as follows:

1. Generate the symmetric key for a dataset.
2. Encrypt the dataset data (CSV) using the symmetric key and store the encrypted file as `<DATASET_NAME>.csv.encr`
3. Encrypt the symmetric key using the asymmetric RSA public key `microdata_public_key.pem` 
   and store the encrypted file as `<DATASET_NAME>.symkey.encr`
4. Gather the encrypted CSV, encrypted symmetric key and metadata (JSON) file in one tar file.

### Unpackage dataset
The `unpackage_dataset()` function will untar and decrypt your dataset using the `microdata_private_key.pem`
RSA private key.

The packaged file has to have the `<DATASET_NAME>.tar` extension. Its contents should be as follows:

```<DATASET_NAME>.json``` : Required medata file.

```<DATASET_NAME>.csv.encr``` : Optional encrypted dataset file.

```<DATASET_NAME>.symkey.encr``` : Optional encrypted file containing the symmetrical key used to decrypt the dataset file. Required if the `.csv.encr` file is present.

Decryption uses the RSA private key located at ```RSA_KEY_DIR```.

The packaged file is then stored in `output_dir/archive/unpackaged` after a successful run or `output_dir/archive/failed` after an unsuccessful run.

## Example
Python script that uses a RSA public key named `microdata_public_key.pem` and packages a dataset:

```py
from pathlib import Path
from microdata_tools import package_dataset

RSA_KEYS_DIRECTORY = Path("tests/resources/rsa_keys")
DATASET_DIRECTORY = Path("tests/resources/input_package/DATASET_1")
OUTPUT_DIRECTORY = Path("tests/resources/output")

package_dataset(
   rsa_keys_dir=RSA_KEYS_DIRECTORY,
   dataset_dir=DATASET_DIRECTORY,
   output_dir=OUTPUT_DIRECTORY,
)
```
