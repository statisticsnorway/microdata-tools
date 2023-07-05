from typing import Union

from microdata_tools.validation.exceptions import UnregisteredUnitTypeError

# When updating this dictionary remember to also
# update the Metadata model with the
# same key value in the enum for unitType
UNIT_ID_TYPE_FOR_UNIT_TYPE = {
    "JOBB": "JOBBID_1",
    "KJORETOY": "KJORETOY_ID",
    "FAMILIE": "FNR",
    "FORETAK": "ORGNR",
    "HUSHOLDNING": "FNR",
    "KOMMUNE": None,
    "KURS": "KURSID",
    "PERSON": "FNR",
    "VIRKSOMHET": "ORGNR",
    "BK_HELSESTASJONSKONSULTASJON": "BK_STASJONS_BESOKS_ID",
}


def get(unit_type: str) -> Union[str, None]:
    try:
        return UNIT_ID_TYPE_FOR_UNIT_TYPE[unit_type]
    except KeyError as e:
        raise UnregisteredUnitTypeError(f"No such unit type: {str(e)}") from e
