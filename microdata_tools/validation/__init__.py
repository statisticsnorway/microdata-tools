def validate_dataset(
    dataset_name: str,
    input_directory: str = "",
    working_directory: str = "",
    keep_temporary_files: bool = False,
    metadata_ref_directory: str = ""
):
    ...


def validate_metadata(
    dataset_name: str,
    input_directory: str = "",
    working_directory: str = "",
    keep_temporary_files: bool = False,
    metadata_ref_directory: str = ""
):
    ...


def inline_metadata(
        metadata_file_path: str,
        metadata_ref_directory: str,
        output_file_path: str = ""
):
    ...
