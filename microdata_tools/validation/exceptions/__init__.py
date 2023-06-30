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


class InvalidDatasetException(Exception):
    def __init__(self, message: str, errors: list):
        self.errors = errors
        Exception.__init__(self, message)
