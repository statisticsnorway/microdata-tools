import json
from datetime import datetime
from pathlib import Path

import pytest

from microdata_tools.validation.steps import metadata_enricher

INPUT_DIR = Path("tests/resources/validation/steps/metadata_enricher")


def test_enrich_with_temporal_coverage_fixed():
    """
    A FIXED dataset has no dates in its data file, so its temporal
    coverage is not derived from the data. The data is assumed to be
    valid up until the time of validation.
    """
    metadata_path = INPUT_DIR / "VALID_FIXED_METADATA.json"
    with open(metadata_path, "r", encoding="utf-8") as f:
        metadata = json.load(f)
        metadata_enricher.enrich_with_temporal_coverage(metadata, {})
        data_revision = metadata["dataRevision"]
        assert data_revision["temporalCoverageStart"] == "1900-01-01"
        assert data_revision[
            "temporalCoverageLatest"
        ] == datetime.now().strftime("%Y-%m-%d")
        assert "temporalStatusDates" not in data_revision


def test_enrich_with_temporal_coverage():
    temporal_data_no_status_dates = {
        "start": "1900-01-01",
        "latest": "2000-01-01",
    }
    temporal_data_with_status_dates = {
        "start": "1900-01-01",
        "latest": "2000-01-01",
        "statusDates": ["1900-01-01", "2000-01-01"],
    }
    metadata_path = INPUT_DIR / "VALID_EVENT_METADATA.json"
    with open(metadata_path, "r", encoding="utf-8") as f:
        metadata = json.load(f)
        metadata_enricher.enrich_with_temporal_coverage(
            metadata, temporal_data_no_status_dates
        )
        data_revision = metadata["dataRevision"]
        assert data_revision["temporalCoverageStart"] == "1900-01-01"
        assert data_revision["temporalCoverageLatest"] == "2000-01-01"
        assert "temporalStatusDates" not in data_revision

    with open(metadata_path, "r", encoding="utf-8") as f:
        metadata = json.load(f)
        metadata_enricher.enrich_with_temporal_coverage(
            metadata, temporal_data_with_status_dates
        )
        data_revision = metadata["dataRevision"]
        assert data_revision["temporalCoverageStart"] == "1900-01-01"
        assert data_revision["temporalCoverageLatest"] == "2000-01-01"
        assert "temporalStatusDates" not in data_revision

    metadata_path = INPUT_DIR / "VALID_STATUS_METADATA.json"
    with open(metadata_path, "r", encoding="utf-8") as f:
        metadata = json.load(f)
        metadata_enricher.enrich_with_temporal_coverage(
            metadata, temporal_data_with_status_dates
        )
        data_revision = metadata["dataRevision"]
        assert data_revision["temporalCoverageStart"] == "1900-01-01"
        assert data_revision["temporalCoverageLatest"] == "2000-01-01"
        assert data_revision["temporalStatusDates"] == [
            "1900-01-01",
            "2000-01-01",
        ]
    with open(metadata_path, "r", encoding="utf-8") as f:
        metadata = json.load(f)
        with pytest.raises(KeyError) as e:
            metadata_enricher.enrich_with_temporal_coverage(
                metadata, temporal_data_no_status_dates
            )
        assert "statusDates" in str(e)
