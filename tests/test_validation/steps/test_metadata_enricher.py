import json
from pathlib import Path

import pytest

from microdata_tools.validation.steps import metadata_enricher

INPUT_DIR = Path("tests/resources/validation/steps/metadata_enricher")


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
