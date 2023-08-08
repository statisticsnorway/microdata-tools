import logging


logger = logging.getLogger()


def enrich_with_temporal_coverage(metadata: dict, temporal_data: dict) -> None:
    logger.debug(
        "Append temporal coverage (start, stop, status dates) to metadata"
    )
    data_revision = metadata["dataRevision"]
    temporality_type = metadata["temporalityType"]
    data_revision["temporalCoverageStart"] = temporal_data["start"]
    data_revision["temporalCoverageLatest"] = temporal_data["latest"]
    if temporality_type == "STATUS":
        temporal_status_dates_list = temporal_data["statusDates"]
        temporal_status_dates_list.sort()
        data_revision["temporalStatusDates"] = temporal_status_dates_list
