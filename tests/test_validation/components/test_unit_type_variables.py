from typing import Optional
from pydantic import BaseModel, conlist

from microdata_tools.validation.model.metadata import (
    MultiLingualString,
    DataType,
    ValueDomain,
)
from microdata_tools.validation.components.unit_type_variables import (
    UNIT_TYPE_VARIABLES,
)


def test_unit_type_variables_format():
    for _, variable in UNIT_TYPE_VARIABLES.items():
        UnitTypeVariable(**variable)


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
