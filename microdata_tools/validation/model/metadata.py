import datetime
from enum import Enum
from typing import Optional, List, Union

from pydantic import BaseModel, Field, conlist, model_validator


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
    TRAFIKKULYKKE = "TRAFIKKULYKKE"
    TRAFIKKULYKKE_PERSON = "TRAFIKKULYKKE_PERSON"
    MALEPUNKT = "MALEPUNKT"
    KRG_KREFTTILFELLE = "KRG_KREFTTILFELLE"


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
    TRAFIKKULYKKE_ID = "TRAFIKKULYKKE_ID"
    TRAFIKKULYKKE_PERSON_ID = "TRAFIKKULYKKE_PERSON_ID"
    MALEPUNKT_ID = "MALEPUNKT_ID"
    KRG_TILFELLEID = "KRG_TILFELLEID"


class MultiLingualString(BaseModel):
    languageCode: LanguageCode
    value: str = Field(min_length=1)


class TemporalEnd(BaseModel):
    description: conlist(MultiLingualString, min_length=1)
    successors: Optional[conlist(str, min_length=1)] = None


class DataRevision(BaseModel, extra="forbid"):
    description: conlist(MultiLingualString, min_length=1)
    temporalEnd: Optional[TemporalEnd] = None


class IdentifierVariable(BaseModel, extra="forbid"):
    unitType: UnitType


class CodeListItem(BaseModel, extra="forbid"):
    code: Union[str, int]
    categoryTitle: conlist(MultiLingualString, min_length=1)
    validFrom: str = Field(min_length=1)
    validUntil: Optional[str] = None

    @model_validator(mode="before")
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

        code = values.get("code", None)
        if isinstance(code, str) and len(code) < 1:
            raise ValueError("String should have at least 1 character")
        return values


class SentinelItem(BaseModel, extra="forbid"):
    code: Union[str, int]
    categoryTitle: conlist(MultiLingualString, min_length=1)

    @model_validator(mode="before")
    @classmethod
    def validate_sentinel_list_item(cls, values):
        code = values.get("code", None)
        if isinstance(code, str) and len(code) < 1:
            raise ValueError("String should have at least 1 character")

        return values


class ValueDomain(BaseModel, extra="forbid"):
    description: Optional[conlist(MultiLingualString, min_length=1)] = None
    measurementType: Optional[str] = None
    measurementUnitDescription: Optional[
        conlist(MultiLingualString, min_length=1)
    ] = None
    uriDefinition: Optional[List[str]] = None
    codeList: Optional[conlist(CodeListItem, min_length=1)] = None
    sentinelAndMissingValues: Optional[List[SentinelItem]] = None

    @model_validator(mode="before")
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
    unitType: Optional[UnitType] = None
    name: conlist(MultiLingualString, min_length=1)
    description: conlist(MultiLingualString, min_length=1)
    dataType: Optional[DataType] = None
    uriDefinition: Optional[List[str]] = None
    format: Optional[str] = None
    valueDomain: Optional[ValueDomain] = None

    @model_validator(mode="before")
    @classmethod
    def validate_measure(cls, values: dict):
        def raise_invalid_with_unit_type(field_name: str):
            raise ValueError(
                f"Can not set a {field_name} in a measure variable "
                "together with a unitType"
            )

        value_domain = values.get("valueDomain", None)
        data_type = values.get("dataType", None)
        unit_type = values.get("unitType", None)
        if unit_type is not None:
            if data_type is not None:
                raise_invalid_with_unit_type("dataType")
            if value_domain is not None:
                raise_invalid_with_unit_type("valueDomain")
        else:
            if data_type is None:
                raise ValueError("Missing dataType in measure variable")
            if value_domain is None:
                raise ValueError("Missing valueDomain in measure variable")

        return values


class Metadata(BaseModel):
    temporalityType: TemporalityType
    sensitivityLevel: SensitivityLevel
    populationDescription: conlist(MultiLingualString, min_length=1)
    spatialCoverageDescription: Optional[
        conlist(MultiLingualString, min_length=1)
    ] = None
    subjectFields: conlist(
        conlist(MultiLingualString, min_length=1), min_length=1
    )
    dataRevision: DataRevision
    identifierVariables: conlist(
        IdentifierVariable, min_length=1, max_length=1
    )
    measureVariables: conlist(MeasureVariable, min_length=1, max_length=1)
