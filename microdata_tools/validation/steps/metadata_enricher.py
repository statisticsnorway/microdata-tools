import logging
from datetime import datetime

logger = logging.getLogger()


def enrich_with_temporal_coverage(metadata: dict, temporal_data: dict) -> None:
    logger.debug(
        "Append temporal coverage (start, stop, status dates) to metadata"
    )
    data_revision = metadata["dataRevision"]
    temporality_type = metadata["temporalityType"]
    if temporality_type == "FIXED":
        # A FIXED dataset has no dates in its data file, so there is no
        # temporal coverage to derive from the data.
        data_revision["temporalCoverageStart"] = "1900-01-01"
        data_revision["temporalCoverageLatest"] = datetime.now().strftime(
            "%Y-%m-%d"
        )
        return
    data_revision["temporalCoverageStart"] = temporal_data["start"]
    data_revision["temporalCoverageLatest"] = temporal_data["latest"]
    if temporality_type == "STATUS":
        temporal_status_dates_list = temporal_data["statusDates"]
        temporal_status_dates_list.sort()
        data_revision["temporalStatusDates"] = temporal_status_dates_list
