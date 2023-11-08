from typing import Union

from microdata_tools.validation.exceptions import UnregisteredUnitTypeError
from microdata_tools.validation.model.metadata import UnitType, UnitIdType

# When updating this dictionary remember to also
# update the Metadata model with the
# same key value in the enum for unitType
UNIT_ID_TYPE_FOR_UNIT_TYPE = {
    UnitType.JOBB: UnitIdType.JOBBID_1,
    UnitType.KJORETOY: UnitIdType.KJORETOY_ID,
    UnitType.FAMILIE: UnitIdType.FNR,
    UnitType.FORETAK: UnitIdType.ORGNR,
    UnitType.HUSHOLDNING: UnitIdType.FNR,
    UnitType.KOMMUNE: None,
    UnitType.KURS: UnitIdType.KURSID,
    UnitType.PERSON: UnitIdType.FNR,
    UnitType.VIRKSOMHET: UnitIdType.ORGNR,
    UnitType.BK_HELSESTASJONSKONSULTASJON: None,
    UnitType.NPR_EPISODE: UnitIdType.NPR_EPISODE_ID,
    UnitType.HKDIR_STUDIESOKNAD: UnitIdType.HKDIR_STUDIESOKNAD_ID,
    UnitType.SOSTIL: UnitIdType.SOSTIL_ID,
}


def get(unit_type: UnitType) -> Union[UnitIdType, None]:
    try:
        return UNIT_ID_TYPE_FOR_UNIT_TYPE[unit_type]
    except KeyError as e:
        raise UnregisteredUnitTypeError(f"No such unit type: {str(e)}") from e
