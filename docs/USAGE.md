# USAGE

## Get started

Install microdata-tools through pip:
```sh
pip install microdata-tools
```

Upgrade to a newer version
```sh
pip install microdata-tools --upgrade
```

Import into your python script like so:
```py
import microdata_tools
```


## Validate dataset

Once you have your metadata and data files ready to go, they should be named and stored like this:
```
my-input-directory/
    MY_DATASET_NAME/
        MY_DATASET_NAME.csv
        MY_DATASET_NAME.json
    MY_OTHER_DATASET/
        MY_OTHER_DATASET.json
```


Import microdata-tools in your script and validate your files:

```py
from microdata_tools import validate_dataset

validation_errors = validate_dataset("MY_DATASET_NAME")

if not validation_errors:
    print("My dataset is valid")
else:
    print("Dataset is invalid :(")
    # You can print your errors like this:
    for error in validation_errors:
        print(error)
```


The input directory is set to the directory of the script by default.
If you wish to use a different directory, you can use the ```input_directory```-parameter:

```py

from microdata_tools import validate_dataset

validation_errors = validate_dataset(
    "MY_DATASET_NAME",
    input_directory="/my/input/directory",
)

if not validation_errors:
    print("My dataset is valid")
else:
    print("Dataset is invalid :(")
```

The validate function will temporarily generate some files in order to validate your dataset. To do this, it will create a working directory in the same location as your script, and delete it once it is done. Therefore, it is important that you have writing permissions in your directory. You can also choose to define the location of this directory yourself using the ```working_directory```-parameter. If you choose to do this, the validate function will only delete the files it generates.


```py

from microdata_tools import validate_dataset

validation_errors = validate_dataset(
    "my-dataset-name",
    input_directory="/my/input/directory",
    working_directory="/my/working/directory"
)

if not validation_errors:
    print("My dataset is valid")
else:
    print("Dataset is invalid :(")
```

If you wish to keep the temporary files after the validation has run, you can do this with the ```keep_temporary_files```-parameter:

```py
from microdata_tools import validate_dataset

validation_errors = validate_dataset(
    "MY_DATASET_NAME",
    input_directory="/my/input/directory",
    working_directory="/my/working/directory",
    keep_temporary_files=True
)

if not validation_errors:
    print("My dataset is valid")
else:
    print("Dataset is invalid :(")
```
 
## Validate metadata
What if your data is not yet done, but you want to start generating and validating your metadata? Keep your files in the same directory structure as described above, minus the csv file.
You can validate the metadata by itself with the validate_metadata function:
```py
from microdata_tools import validate_metadata

validation_errors = validate_metadata(
    "MY_DATASET_NAME",
    input_directory="my/input/directory"
)

if not validation_errors:
    print("Metadata looks good")
else:
    print("Invalid metadata :(")
```
This will only check if all required fields are present, and that the metadata follows the correct structure. Since it does not have the data file it can not do the more complex validations. It may still be a helpful way to discover errors early.
