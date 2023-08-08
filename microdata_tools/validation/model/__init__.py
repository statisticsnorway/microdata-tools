# pylint: disable=raise-missing-from
import logging
from typing import Dict

import pydantic

from microdata_tools.validation.exceptions import ValidationError
from microdata_tools.validation.model.metadata import Metadata


logger = logging.getLogger()


def _format_pydantic_error(error: dict) -> str:
    location = "->".join(
        loc
        for loc in error["loc"]
        if loc != "__root__" and not isinstance(loc, int)
    )
    return f'{location}: {error["msg"]}'


def validate_metadata_model(metadata_json: Dict) -> Metadata:
    try:
        return Metadata(**metadata_json)
    except pydantic.ValidationError as e:
        logger.exception(e)
        error_messages = [
            _format_pydantic_error(error) for error in e.errors()
        ]
        raise ValidationError("metadata file", errors=error_messages)
    except Exception as e:
        logger.exception(e)
        raise e
