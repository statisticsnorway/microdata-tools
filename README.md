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

### Validation

Once you have your metadata and data files ready to go, they should be named and stored like this:
```
my-input-directory/
    MY_DATASET_NAME/
        MY_DATASET_NAME.csv
        MY_DATASET_NAME.json
```
Note that the filename only allows upper case letters A-Z, number 0-9 and underscores.


Import microdata-tools in your script and validate your files:
```py
from microdata_tools import validate_dataset

validation_errors = validate_dataset(
    "MY_DATASET_NAME",
    input_directory="path/to/my-input-directory"
)

if not validation_errors:
    print("My dataset is valid")
else:
    print("Dataset is invalid :(")
    # You can print your errors like this:
    for error in validation_errors:
        print(error)
```

 For a more in-depth explanation of usage visit [the usage documentation](https://statisticsnorway.github.io/microdata-tools/usage.md).

 ### Data format description
A dataset as defined in microdata consists of one data file, and one metadata file.

The data file is a csv file seperated by semicolons. A valid example would be:
```csv
000000000000001;123;2020-01-01;2020-12-31;
000000000000002;123;2020-01-01;2020-12-31;
000000000000003;123;2020-01-01;2020-12-31;
000000000000004;123;2020-01-01;2020-12-31;
```
Read more about the data format and columns in [the documentation](https://statisticsnorway.github.io/microdata-tools/).

The metadata files should be in json format. The requirements for the metadata is best described through the [Pydantic model](/microdata_tools/validation/model/metadata.py), [the examples](/docs/examples), and [the metadata model](https://statisticsnorway.github.io/microdata-tools/metadata-model/).
