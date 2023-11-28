import os
import shutil
import json
import copy
from typing import Optional
from pydantic import BaseModel, conlist

from microdata_tools import validate_metadata
from microdata_tools.validation.model.metadata import (
    MultiLingualString,
    DataType,
    ValueDomain,
)
from microdata_tools.validation.components.unit_type_variables import (
    UNIT_TYPE_VARIABLES,
)

INPUT_DIR = "tests/resources/validation/components/unit_type_variables"


class UnitType(BaseModel):
    shortName: str
    name: conlist(MultiLingualString, min_items=1)
    requiresPseudonymization: bool
    description: conlist(MultiLingualString, min_items=1)


class UnitTypeVariable(BaseModel):
    shortName: str
    name: conlist(MultiLingualString, min_items=1)
    description: conlist(MultiLingualString, min_items=1)
    dataType: DataType
    format: Optional[str]
    unitType: UnitType
    valueDomain: ValueDomain


def setup_module():
    with open(f"{INPUT_DIR}/metadata_template.json") as f:
        template = json.load(f)
    for unit_type_name in UNIT_TYPE_VARIABLES.keys():
        os.mkdir(f"{INPUT_DIR}/TEST_{unit_type_name}")
        unit_type_metadata = copy.deepcopy(template)
        unit_type_metadata["identifierVariables"][0][
            "unitType"
        ] = unit_type_name
        with open(
            f"{INPUT_DIR}/TEST_{unit_type_name}/TEST_{unit_type_name}.json",
            "w",
        ) as f:
            json.dump(unit_type_metadata, f)


def teardown_module():
    for dir_name in os.listdir(INPUT_DIR):
        if os.path.isdir(f"{INPUT_DIR}/{dir_name}"):
            shutil.rmtree(f"{INPUT_DIR}/{dir_name}")


def test_unit_type_variables_format():
    for _, variable in UNIT_TYPE_VARIABLES.items():
        UnitTypeVariable(**variable)


def test_unit_type_variables_in_metadata():
    for unit_type_name in UNIT_TYPE_VARIABLES.keys():
        errors = validate_metadata(
            f"TEST_{unit_type_name}", input_directory=INPUT_DIR
        )
        assert not errors
