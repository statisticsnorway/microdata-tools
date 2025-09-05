import pytest

from microdata_tools import get_unit_id_type_for_unit_type
from microdata_tools.validation.exceptions import UnregisteredUnitTypeError
from microdata_tools.validation.model.metadata import UnitIdType, UnitType


def test_get_unit_id_type_for_unit_type():
    assert UnitIdType.FNR == get_unit_id_type_for_unit_type(UnitType.PERSON)
    assert UnitIdType.FNR == get_unit_id_type_for_unit_type(UnitType.FAMILIE)
    assert UnitIdType.ORGNR == get_unit_id_type_for_unit_type(
        UnitType.VIRKSOMHET
    )
    assert get_unit_id_type_for_unit_type(UnitType.KOMMUNE) is None
    assert (
        get_unit_id_type_for_unit_type(UnitType.BK_HELSESTASJONSKONSULTASJON)
        is None
    )

    with pytest.raises(UnregisteredUnitTypeError) as e:
        get_unit_id_type_for_unit_type("NOT A UNIT TYPE")  # pyright: ignore[reportArgumentType]
    assert "No such unit type" in str(e)
