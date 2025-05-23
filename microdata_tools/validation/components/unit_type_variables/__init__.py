from copy import deepcopy
from pathlib import Path

from microdata_tools.validation.exceptions import InvalidIdentifierType
from microdata_tools.validation.adapter.local_storage import load_json


UNIT_TYPE_VARIABLES_DIR = Path(__file__).parent
UNIT_TYPE_VARIABLES = {
    "JOBB": load_json(UNIT_TYPE_VARIABLES_DIR / "JOBB.json"),
    "KJORETOY": load_json(UNIT_TYPE_VARIABLES_DIR / "KJORETOY.json"),
    "FAMILIE": load_json(UNIT_TYPE_VARIABLES_DIR / "FAMILIE.json"),
    "FORETAK": load_json(UNIT_TYPE_VARIABLES_DIR / "FORETAK.json"),
    "HUSHOLDNING": load_json(UNIT_TYPE_VARIABLES_DIR / "HUSHOLDNING.json"),
    "KOMMUNE": load_json(UNIT_TYPE_VARIABLES_DIR / "KOMMUNE.json"),
    "KURS": load_json(UNIT_TYPE_VARIABLES_DIR / "KURS.json"),
    "PERSON": load_json(UNIT_TYPE_VARIABLES_DIR / "PERSON.json"),
    "VIRKSOMHET": load_json(UNIT_TYPE_VARIABLES_DIR / "VIRKSOMHET.json"),
    "NPR_EPISODE": load_json(UNIT_TYPE_VARIABLES_DIR / "NPR_EPISODE.json"),
    "NPR_SYKEHUSOPPHOLD": load_json(
        UNIT_TYPE_VARIABLES_DIR / "NPR_SYKEHUSOPPHOLD.json"
    ),
    "BK_HELSESTASJONSKONSULTASJON": load_json(
        UNIT_TYPE_VARIABLES_DIR / "BK_HELSESTASJONSKONSULTASJON.json"
    ),
    "HKDIR_HYU_OPPMELDING": load_json(
        UNIT_TYPE_VARIABLES_DIR / "HKDIR_HYU_OPPMELDING.json"
    ),
    "HKDIR_STUDIESOKNAD": load_json(
        UNIT_TYPE_VARIABLES_DIR / "HKDIR_STUDIESOKNAD.json"
    ),
    "SOSTIL": load_json(UNIT_TYPE_VARIABLES_DIR / "SOSTIL.json"),
    "TRAFIKKULYKKE": load_json(UNIT_TYPE_VARIABLES_DIR / "TRAFIKKULYKKE.json"),
    "MALEPUNKT": load_json(UNIT_TYPE_VARIABLES_DIR / "MALEPUNKT.json"),
    "TRAFIKKULYKKE_PERSON": load_json(
        UNIT_TYPE_VARIABLES_DIR / "TRAFIKKULYKKE_PERSON.json"
    ),
    "KRG_KREFTTILFELLE": load_json(
        UNIT_TYPE_VARIABLES_DIR / "KRG_KREFTTILFELLE.json"
    ),
    "FENGSLINGER_TILGANG": load_json(
        UNIT_TYPE_VARIABLES_DIR / "FENGSLINGER_TILGANG.json"
    ),
    "SOESKEN": load_json(UNIT_TYPE_VARIABLES_DIR / "SOESKEN.json"),
    "BEDRIFT": load_json(UNIT_TYPE_VARIABLES_DIR / "BEDRIFT.json"),
}


def get(unit_type: str):
    try:
        return deepcopy(UNIT_TYPE_VARIABLES[unit_type])
    except KeyError as e:
        raise InvalidIdentifierType(unit_type) from e
