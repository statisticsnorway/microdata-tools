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
    "BK_HELSESTASJONSKONSULTASJON": load_json(
        UNIT_TYPE_VARIABLES_DIR / "BK_HELSESTASJONSKONSULTASJON.json"
    ),
    "HKDIR_STUDIESOKNAD": load_json(
        UNIT_TYPE_VARIABLES_DIR / "HKDIR_STUDIESOKNAD.json"
    ),
    "SOSTIL": load_json(UNIT_TYPE_VARIABLES_DIR / "SOSTIL.json"),
}


def get(unit_type: str):
    try:
        return deepcopy(UNIT_TYPE_VARIABLES[unit_type])
    except KeyError as e:
        raise InvalidIdentifierType(unit_type) from e
