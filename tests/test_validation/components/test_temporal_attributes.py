import pytest

from microdata_tools.validation.components import temporal_attributes
from microdata_tools.validation.exceptions import InvalidTemporalityType


def test_generate_temporal_attributes_valid_temporality():
    for temporality in ["FIXED", "STATUS", "ACCUMULATED", "EVENT"]:
        start = temporal_attributes.generate_start_time_attribute(temporality)
        stop = temporal_attributes.generate_stop_time_attribute(temporality)
        assert (start["dataType"], stop["dataType"]) == ("DATE", "DATE")
        assert start["shortName"] == "START"
        assert stop["shortName"] == "STOP"
        assert start["variableRole"] == "Start"
        assert stop["variableRole"] == "Stop"
        assert "name" in start.keys()
        assert "description" in start.keys()
        assert "name" in stop.keys()
        assert "description" in stop.keys()


def test_generate_temporal_attributes_invalid_type():
    with pytest.raises(InvalidTemporalityType) as e:
        temporal_attributes.generate_start_time_attribute("OCCASIONAL")
    assert "OCCASIONAL" in str(e)

    with pytest.raises(InvalidTemporalityType) as e:
        temporal_attributes.generate_stop_time_attribute(None)
    assert "None" in str(e)
