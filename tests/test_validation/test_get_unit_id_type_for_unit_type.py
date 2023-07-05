import pytest

from microdata_tools import get_unit_id_type_for_unit_type
from microdata_tools.validation.exceptions import UnregisteredUnitTypeError


def test_get_unit_id_type_for_unit_type():
    assert "FNR" == get_unit_id_type_for_unit_type("PERSON")
    assert "FNR" == get_unit_id_type_for_unit_type("FAMILIE")
    assert "ORGNR" == get_unit_id_type_for_unit_type("VIRKSOMHET")
    assert get_unit_id_type_for_unit_type("KOMMUNE") is None

    with pytest.raises(UnregisteredUnitTypeError) as e:
        get_unit_id_type_for_unit_type("NOT A UNIT TYPE")
    assert "No such unit type" in str(e)
