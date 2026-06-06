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
2. Encrypt the dataset data (CSV) using an AES-256-GCM symmetric key and store the encrypted file as `<DATASET_NAME>.csv.encr`
3. Encrypt the symmetric key using HPKE with the combined ML-KEM-768/X25519 public key from `microdata_public_key.pem` and store the resulting HPKE ciphertext as <DATASET_NAME>.kem.encr
4. Gather the encrypted CSV, ciphertext file and metadata (JSON) file in one tar file.

### Unpackage dataset
The `unpackage_dataset()` function will untar and your dataset and use the combined ML-KEM-768/X25519 private key from `microdata_private_key.pem` to recover the symmetric key, which is then used to decrypt the dataset.


The packaged file has to have the `<DATASET_NAME>.tar` extension. Its contents should be as follows:

```<DATASET_NAME>.json``` : Required medata file.

```<DATASET_NAME>.csv.encr``` : Optional encrypted dataset file.

```<DATASET_NAME>.kem.encr``` : Optional HPKE ciphertext file containing the encrypted symmetric key required to decrypt the dataset file. Required if the `.csv.encr` file is present.

Decryption uses the combined ML-KEM-768/X25519 private key located at ```PRIVATE_KEY_DIR``` to recover the symmetric decryption key.

The packaged file is then stored in `output_dir/archive/unpackaged` after a successful run or `output_dir/archive/failed` after an unsuccessful run.


## Example

Store your metadata and data files according to the structure described above, and put
the provided public key in a directory of your choice.
Then:

```py
from pathlib import Path
from microdata_tools import package_dataset

package_dataset(
    public_key_dir=Path("path/to/key_directory"),
    dataset_dir=Path("path/to/MY_DATASET_NAME"),
    output_dir=Path("path/to/output"),
)
```

This produces `path/to/output/MY_DATASET_NAME.tar`, which can be uploaded to microdata.


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

 For a more in-depth explanation of usage visit [the usage documentation](/microdata-tools/USAGE).

### Data format description
A dataset as defined in microdata consists of one data file, and one metadata file.

The data file is a csv file seperated by semicolons. A valid example would be:
```csv
000000000000001;123;2020-01-01;2020-12-31;
000000000000002;123;2020-01-01;2020-12-31;
000000000000003;123;2020-01-01;2020-12-31;
000000000000004;123;2020-01-01;2020-12-31;
```

The metadata files should be in json format. The requirements for the metadata is best described through the [Pydantic model](https://github.com/statisticsnorway/microdata-tools/blob/main/microdata_tools/validation/model/metadata.py), [the examples](https://github.com/statisticsnorway/microdata-tools/tree/main/docs/examples) and [the metadata model](https://statisticsnorway.github.io/microdata-tools/metadata-model/)