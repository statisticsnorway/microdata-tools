import datetime
from enum import Enum
from typing import Optional, List, Union

from pydantic import BaseModel, conlist, root_validator, Extra


class TemporalityType(str, Enum):
    FIXED = "FIXED"
    STATUS = "STATUS"
    ACCUMULATED = "ACCUMULATED"
    EVENT = "EVENT"


class DataType(str, Enum):
    STRING = "STRING"
    LONG = "LONG"
    DATE = "DATE"
    DOUBLE = "DOUBLE"


class SensitivityLevel(str, Enum):
    PERSON_GENERAL = "PERSON_GENERAL"
    PERSON_SPECIAL = "PERSON_SPECIAL"
    PUBLIC = "PUBLIC"
    NONPUBLIC = "NONPUBLIC"


class LanguageCode(str, Enum):
    no = "no"
    nb = "nb"
    nn = "nn"
    en = "en"


class UnitType(str, Enum):
    JOBB = "JOBB"
    KJORETOY = "KJORETOY"
    FAMILIE = "FAMILIE"
    FORETAK = "FORETAK"
    HUSHOLDNING = "HUSHOLDNING"
    KOMMUNE = "KOMMUNE"
    KURS = "KURS"
    PERSON = "PERSON"
    VIRKSOMHET = "VIRKSOMHET"
    BK_HELSESTASJONSKONSULTASJON = "BK_HELSESTASJONSKONSULTASJON"
    NPR_EPISODE = "NPR_EPISODE"
    HKDIR_STUDIESOKNAD = "HKDIR_STUDIESOKNAD"
    SOSTIL = "SOSTIL"


class UnitIdType(str, Enum):
    JOBBID_1 = "JOBBID_1"
    KJORETOY_ID = "KJORETOY_ID"
    FNR = "FNR"
    ORGNR = "ORGNR"
    KURSID = "KURSID"
    BK_STASJONS_BESOKS_ID = "BK_STASJONS_BESOKS_ID"
    HKDIR_STUDIESOKNAD_ID = "HKDIR_STUDIESOKNAD_ID"
    NPR_EPISODE_ID = "NPR_EPISODE_ID"
    SOSTIL_ID = "SOSTIL_ID"


class MultiLingualString(BaseModel):
    languageCode: LanguageCode
    value: str


class DataRevision(BaseModel, extra=Extra.forbid):
    description: conlist(MultiLingualString, min_items=1)
    temporalEndOfSeries: bool


class IdentifierVariable(BaseModel, extra=Extra.forbid):
    unitType: UnitType


class CodeListItem(BaseModel, extra=Extra.forbid):
    code: str
    categoryTitle: conlist(MultiLingualString, min_items=1)
    validFrom: str
    validUntil: Optional[Union[str, None]]

    @root_validator(skip_on_failure=True)
    @classmethod
    def validate_code_list_item(cls, values):
        def validate_date_string(field_name: str, date_string: str):
            try:
                datetime.datetime(
                    int(date_string[:4]),
                    int(date_string[5:7]),
                    int(date_string[8:10]),
                )
            except ValueError as e:
                raise ValueError(
                    f'Invalid {field_name} date for {values["code"]}. '
                    "Date format: YYYY-MM-DD"
                ) from e

        validate_date_string("validFrom", values["validFrom"])
        if values.get("validUntil", None) is not None:
            validate_date_string("validUntil", values["validUntil"])
        return values


class SentinelItem(BaseModel, extra=Extra.forbid):
    code: str
    categoryTitle: conlist(MultiLingualString, min_items=1)


class ValueDomain(BaseModel, extra=Extra.forbid):
    description: Optional[conlist(MultiLingualString, min_items=1)]
    measurementType: Optional[str]
    measurementUnitDescription: Optional[
        conlist(MultiLingualString, min_items=1)
    ]
    uriDefinition: Optional[List[Union[str, None]]]
    codeList: Optional[conlist(CodeListItem, min_items=1)]
    sentinelAndMissingValues: Optional[List[SentinelItem]]

    @root_validator(skip_on_failure=True)
    @classmethod
    def validate_value_domain(cls, values: dict):
        def raise_invalid_with_code_list(field_name: str):
            raise ValueError(
                f"Can not add a {field_name} in a valuedomain with a codeList"
            )

        if values.get("codeList", None) is not None:
            if values.get("description", None) is not None:
                raise_invalid_with_code_list("description")
            if values.get("measurementType", None) is not None:
                raise_invalid_with_code_list("measurementType")
            if values.get("measurementUnitDescription", None) is not None:
                raise_invalid_with_code_list("measurementUnitDescription")
        elif values.get("description", None) is not None:
            if values.get("sentinelAndMissingValues", None) is not None:
                raise ValueError(
                    "Can not add sentinelAndMissingValues "
                    "in valuedomain with a description"
                )
        else:
            raise ValueError(
                "A valueDomain must contain either a codeList "
                "or a description"
            )
        return values


class MeasureVariable(BaseModel):
    unitType: Optional[UnitType]
    name: conlist(MultiLingualString, min_items=1)
    description: conlist(MultiLingualString, min_items=1)
    dataType: Optional[DataType]
    uriDefinition: Optional[List[Union[str, None]]]
    format: Optional[str]
    valueDomain: Optional[ValueDomain]

    @root_validator(skip_on_failure=True)
    @classmethod
    def validate_measure(cls, values: dict):
        def raise_invalid_with_unit_type(field_name: str):
            raise ValueError(
                f"Can not set a {field_name} in a measure variable "
                "together with a unitType"
            )

        if values.get("unitType", None) is not None:
            if values.get("dataType", None) is not None:
                raise_invalid_with_unit_type("dataType")
            if values.get("valueDomain", None) is not None:
                raise_invalid_with_unit_type("valueDomain")
        else:
            if values.get("dataType", None) is None:
                raise ValueError("Missing dataType in measure variable")
            if values.get("valueDomain", None) is None:
                raise ValueError("Missing valueDomain in measure variable")
        return values


class Metadata(BaseModel):
    temporalityType: TemporalityType
    sensitivityLevel: SensitivityLevel
    populationDescription: conlist(MultiLingualString, min_items=1)
    spatialCoverageDescription: Optional[
        conlist(MultiLingualString, min_items=1)
    ]
    subjectFields: conlist(
        conlist(MultiLingualString, min_items=1), min_items=1
    )
    dataRevision: DataRevision
    identifierVariables: conlist(IdentifierVariable, min_items=1, max_items=1)
    measureVariables: conlist(MeasureVariable, min_items=1, max_items=1)
