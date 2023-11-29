import os
from typing import Dict
from pathlib import Path

from pyarrow import parquet, Table, dataset


PARQUET_DIR = Path(
    "tests/resources/validation/steps/dataset_validator/parquet"
)


def _dataset_from_dict(name: str, table_dict: Dict):
    parquet_path = PARQUET_DIR / f"{name}.parquet"
    parquet.write_table(Table.from_pydict(table_dict), parquet_path)
    return dataset.dataset(parquet_path)


def _delete_parquet_files():
    parquet_files = [f for f in os.listdir(PARQUET_DIR) if "parquet" in f]
    for f in parquet_files:
        try:
            os.remove(PARQUET_DIR / f)
        except Exception:
            ...


# ------------------
# MEASURE: CODE LIST
# ------------------
FIXED_STRING_CODELIST = [
    {
        "code": "1",
        "categoryTitle": [{"languageCode": "no", "value": "Ugift"}],
        "validFrom": "1926-01-01",
    },
    {
        "code": "2",
        "categoryTitle": [{"languageCode": "no", "value": "Gift"}],
        "validFrom": "1926-01-01",
    },
]
FIXED_STRING_CODELIST_SENTINEL = [
    {
        "code": "0",
        "categoryTitle": [
            {"languageCode": "no", "value": "Sivilstand ukjent"}
        ],
    }
]
FIXED_STRING_CODELIST_DS = _dataset_from_dict(
    "FIXED_STRING_CODELIST_DS",
    {
        "unit_id": ["1", "2", "3", "4"],
        "value": ["0", "1", "2", "1"],
        "start_year": [None] * 4,
        "start_epoch_days": [None] * 4,
        "stop_epoch_days": [18262] * 4,
    },
)
FIXED_STRING_CODELIST_INVALID_DS = _dataset_from_dict(
    "FIXED_STRING_CODELIST_INVALID_DS",
    {
        "unit_id": ["1", "2", "3", "4"],
        "value": ["0", "1", "2", "3"],
        "start_year": [None] * 4,
        "start_epoch_days": [None] * 4,
        "stop_epoch_days": [18262] * 4,
    },
)


# -------------------
# MEASURE: DATA TYPE
# -------------------
_FIXED_DS_TEMPLATE = {
    "unit_id": ["1", "2", "3", "4"],
    "start_year": [None] * 4,
    "start_epoch_days": [None] * 4,
    "stop_epoch_days": [18262] * 4,
}


FIXED_STRING_DS = _dataset_from_dict(
    "FIXED_STRING_DS",
    {
        **_FIXED_DS_TEMPLATE,
        "value": ["0", "b", "2c", "3"],
    },
)

FIXED_STRING_INVALID_DS = _dataset_from_dict(
    "FIXED_STRING_INVALID_DS",
    {
        **_FIXED_DS_TEMPLATE,
        "value": ["ab", "", None, "3"],
    },
)

FIXED_LONG_DS = _dataset_from_dict(
    "FIXED_LONG_DS",
    {
        **_FIXED_DS_TEMPLATE,
        "value": [0, -1, 1, 2],
    },
)

FIXED_LONG_INVALID_DS = _dataset_from_dict(
    "FIXED_LONG_INVALID_DS",
    {
        **_FIXED_DS_TEMPLATE,
        "value": [0, -1, 1, None],
    },
)

FIXED_DOUBLE_DS = _dataset_from_dict(
    "FIXED_DOUBLE_DS",
    {
        **_FIXED_DS_TEMPLATE,
        "value": [0.1, -0.32, 0.003, -0.1],
    },
)

FIXED_DOUBLE_INVALID_DS = _dataset_from_dict(
    "FIXED_DOUBLE_INVALID_DS",
    {
        **_FIXED_DS_TEMPLATE,
        "value": [0.1, -0.32, 0.003, None],
    },
)

FIXED_DATE_DS = _dataset_from_dict(
    "FIXED_DATE_DS",
    {
        **_FIXED_DS_TEMPLATE,
        "value": [18626, 18626, 18626, 18626],
    },
)

FIXED_DATE_INVALID_DS = _dataset_from_dict(
    "FIXED_DATE_INVALID_DS",
    {
        **_FIXED_DS_TEMPLATE,
        "value": [18626, 18626, 18626, None],
    },
)

# -------------------------
# TEMPORALITY: FIXED
# -------------------------
_FIXED_VALID_DICT = {
    "unit_id": ["1", "2", "3", "4"],
    "value": ["1", "2", "3", "4"],
    "start_year": [None] * 4,
    "start_epoch_days": [None] * 4,
    "stop_epoch_days": [18262] * 4,
}
FIXED_VALID_DS = _dataset_from_dict("FIXED_VALID_DS", _FIXED_VALID_DICT)
FIXED_INVALID_START_DS = _dataset_from_dict(
    "FIXED_INVALID_START_DS",
    {
        **_FIXED_VALID_DICT,
        "start_year": ["2020"] * 4,
        "start_epoch_days": [18626] * 4,
    },
)
FIXED_INVALID_DUPLICATES_DS = _dataset_from_dict(
    "FIXED_INVALID_DUPLICATES_DS",
    {**_FIXED_VALID_DICT, "unit_id": ["1", "2", "3", "3"]},
)
# -------------------------
# TEMPORALITY: STATUS
# -------------------------
_STATUS_VALID_DICT = {
    "unit_id": ["1", "2", "3", "4"],
    "value": ["1", "2", "3", "4"],
    "start_year": ["2020"] * 4,
    "start_epoch_days": [18626] * 4,
    "stop_epoch_days": [18626] * 4,
}
STATUS_VALID_DS = _dataset_from_dict("STATUS_VALID_DS", _STATUS_VALID_DICT)
STATUS_INVALID_START_STOP_DS = _dataset_from_dict(
    "STATUS_INVALID_START_STOP_DS",
    {**_STATUS_VALID_DICT, "start_epoch_days": [18727] * 4},
)
STATUS_INVALID_DUPLICATES_DS = _dataset_from_dict(
    "STATUS_INVALID_DUPLICATES_DS",
    {**_STATUS_VALID_DICT, "unit_id": ["1", "2", "3", "3"]},
)

# -------------------------
# TEMPORALITY: EVENT
# -------------------------
_EVENT_VALID_DICT = {
    "unit_id": ["1", "1", "2", "3"],
    "value": ["1", "2", "3", "4"],
    "start_year": ["2020"] * 4,
    "start_epoch_days": [18626, 18671, 18626, 18626],
    "stop_epoch_days": [18670, 18680, 18627, None],
}
EVENT_VALID_DS = _dataset_from_dict("EVENT_VALID_DS", _EVENT_VALID_DICT)
EVENT_INVALID_START_DS = _dataset_from_dict(
    "EVENT_INVALID_START_DS",
    {**_EVENT_VALID_DICT, "start_epoch_days": [None, 18271, 18626, 18626]},
)
EVENT_INVALID_TIMESPANS_DS = _dataset_from_dict(
    "EVENT_INVALID_TIMESPANS_DS",
    {**_EVENT_VALID_DICT, "start_epoch_days": [18626, 18627, 18626, 18626]},
)

# -------------------------
# TEMPORALITY: ACCUMULATED
# -------------------------
_ACCUMULATED_VALID_DICT = {
    "unit_id": ["1", "1", "2", "3"],
    "value": ["1", "2", "3", "4"],
    "start_year": ["2020"] * 4,
    "start_epoch_days": [18626, 18671, 18626, 18626],
    "stop_epoch_days": [18670, 18672, 18627, 18627],
}
ACCUMULATED_VALID_DS = _dataset_from_dict(
    "ACCUMULATED_VALID_DS", _ACCUMULATED_VALID_DICT
)
ACCUMULATED_INVALID_START_STOP_DS = _dataset_from_dict(
    "ACCUMULATED_INVALID_START_STOP_DS",
    {
        **_ACCUMULATED_VALID_DICT,
        "start_epoch_days": [18626, None, 18264, 18262],
        "stop_epoch_days": [None, 18280, 18263, 18263],
    },
)
ACCUMULATED_INVALID_TIMESPANS_DS = _dataset_from_dict(
    "ACCUMULATED_INVALID_TIMESPANS_DS",
    {
        **_ACCUMULATED_VALID_DICT,
        "start_epoch_days": [18626, 18627, 18626, 18626],
    },
)

# ----------------------
# TOO MANY ERRORS
# ----------------------
TOO_MANY_ERRORS_CODELIST = [
    {
        "code": "1",
        "categoryTitle": [{"languageCode": "no", "value": "Valid"}],
        "validFrom": "1926-01-01",
    },
]

_TOO_MANY_ERRORS_DICT = {
    "unit_id": [str(i) for i in range(60)],
    "value": [str(i) for i in range(60)],
    "start_year": ["2020"] * 60,
    "start_epoch_days": [18626] * 60,
    "stop_epoch_days": [18670] * 60,
}
TOO_MANY_ERRORS_DS = _dataset_from_dict(
    "TOO_MANY_ERRORS_DS", _TOO_MANY_ERRORS_DICT
)
