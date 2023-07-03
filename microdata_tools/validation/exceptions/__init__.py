class UnregisteredUnitTypeError(Exception):
    ...


class ParseMetadataError(Exception):
    ...


class InvalidTemporalityType(Exception):
    ...


class InvalidIdentifierType(Exception):
    ...


class InvalidDatasetName(Exception):
    ...


class ValidationError(Exception):
    errors: list[str] = []

    def __init__(self, dataset_name: str, errors: list[str]):
        self.errors = errors
        super().__init__(f"Errors found while validating {dataset_name}")
