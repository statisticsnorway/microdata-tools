# Getting started

This guide shows how to prepare, validate and package a dataset for microdata.no using `microdata-tools`.

A dataset consists of a metadata file (`.json`) and, in most cases, a data file (`.csv`). The JSON file describes what the data represents. The CSV file contains the actual values. Once your dataset passes validation, `microdata-tools` encrypts the CSV file and packages both files into a `.tar` archive ready for upload.

## Install microdata-tools

`microdata-tools` can be installed from PyPI using pip:
```
pip install microdata-tools
```

To upgrade an existing installation:
```
pip install --upgrade microdata-tools
```

## Prepare the files

Your metadata and data files should be named and stored like this:
```
my-input-directory/
    MY_DATASET_NAME/
        MY_DATASET_NAME.csv
        MY_DATASET_NAME.json
    MY_OTHER_DATASET/
        MY_OTHER_DATASET.csv
        MY_OTHER_DATASET.json
```

The dataset name may only contain upper case letters A-Z, numbers 0-9 and underscores, and must be used consistently as the directory name, the CSV filename and the JSON filename.

You can prepare several datasets at once by placing multiple dataset directories inside your input directory.

### The data file

The data file is a CSV file separated by semicolons, with no header row. The columns must be in this order: identifier, measure, start date, stop date, and a reserved empty column. A valid example would be:
```csv
000000000000001;123;2020-01-01;2020-12-31;
000000000000002;123;2020-01-01;2020-12-31;
000000000000003;123;2020-01-01;2020-12-31;
000000000000004;123;2020-01-01;2020-12-31;
```

Dates use the format `YYYY-MM-DD`. Which dates you fill in depends on the dataset's `temporalityType`.

For example, a dataset with `temporalityType` `ACCUMULATED`, showing income for a calendar year, could be written as:
```csv
10000000000001;750000;2025-01-01;2025-12-31;
```

For datasets with `temporalityType` `FIXED`, the value does not change over time, so there is no time period to describe — both the start and stop columns should be left empty:
```csv
000000000000001;123;;;
000000000000002;123;;;
000000000000003;123;;;
000000000000004;123;;;
```
On microdata.no, such variables are shown with an infinite validity period ("∞"), see [BEFOLKNING_KJOENN](https://microdata.no/discovery/variable/no.ssb.fdb/56/BEFOLKNING_KJOENN) for an example.

See [the data file format](data-file-format.md) for the full validation rules for `FIXED`, `STATUS`, `ACCUMULATED` and `EVENT`, and [the example datasets](https://github.com/statisticsnorway/microdata-tools/tree/main/docs/examples) for complete CSV and JSON files.

### The metadata file

The JSON file describes the dataset as a whole, the unit identified in the first CSV column, and the variable represented by the second column. It also describes how to interpret the variable's values.

See [the metadata model](metadata-model.md) for a full description of every field, or go straight to [the example datasets](https://github.com/statisticsnorway/microdata-tools/tree/main/docs/examples) for complete, valid JSON files.

## Validate the dataset

Create a Python script and run `validate_dataset()` with the dataset name and the directory containing the dataset directory:
```py
from microdata_tools import validate_dataset

validation_errors = validate_dataset(
    "MY_DATASET_NAME", input_directory="path/to/my-input-directory"
)

if not validation_errors:
    print("My dataset is valid")
else:
    print("Dataset is invalid :(")
    # You can print your errors like this:
    for error in validation_errors:
        print(error)
```

An empty list means that validation found no errors. If errors are reported, correct the CSV or JSON file and run the script again. Dataset validation checks both files, including whether the data agrees with the relevant metadata and time rules.


### Working and temporary files

`validate_dataset()` creates a working directory to generate some temporary files while it validates your dataset, and deletes it once it is done. By default this working directory is created next to your script, so make sure you have write permission there. You can control this with two optional parameters:

* `working_directory`: choose where the working directory is created. If you set this yourself, `microdata-tools` will only delete the files it generated there.
* `keep_temporary_files`: set to `True` to keep the generated files after validation instead of deleting them.

```py
from microdata_tools import validate_dataset

validation_errors = validate_dataset(
    "MY_DATASET_NAME",
    input_directory="/my/input/directory",
    working_directory="/my/working/directory",
    keep_temporary_files=True,
)
```

### Validate metadata only

If your data file isn't ready yet, keep your metadata file in the same directory structure described above, minus the CSV file, and validate it on its own with `validate_metadata()`:
```py
from microdata_tools import validate_metadata

validation_errors = validate_metadata(
    "MY_DATASET_NAME", input_directory="path/to/my-input-directory"
)

if not validation_errors:
    print("Metadata looks good")
else:
    print("Invalid metadata :(")
```

This only checks that all required fields are present and that the metadata follows the correct structure. Since there is no data file, it can't perform the more complete checks (like whether the data agrees with the metadata) — you will still need to run `validate_dataset()` once the CSV file is ready.

## Package the dataset

The `package_dataset()` function encrypts your data file and packages it together with your metadata file into a `.tar` archive, ready to be uploaded to microdata.no.

Put the public key supplied by microdata.no in a directory of your choice, then pass the paths to the key directory, the dataset directory and the desired output directory to `package_dataset()`:

```py
from pathlib import Path
from microdata_tools import package_dataset

package_dataset(
    public_key_dir=Path("path/to/key_directory"),
    dataset_dir=Path("path/to/MY_DATASET_NAME"),
    output_dir=Path("path/to/output"),
)
```

This produces `path/to/output/MY_DATASET_NAME.tar`, which can be uploaded to microdata.no.

Your data file is encrypted before it ever leaves your machine, using the public key provided by microdata.no — nobody but microdata.no can decrypt it. See [the packaging format](packaging-format.md) if you'd like to know exactly how this works.

## Where to go next

* [The metadata model](metadata-model.md) — a full field-by-field reference for the metadata JSON file.
* [The data file format](data-file-format.md) — a full reference for the CSV data file, including validation rules.
* [The packaging format](packaging-format.md) — how your data is encrypted and packaged.
* [Report an issue](issue_templates/issue_template_en.md) — found a bug or have a question?
