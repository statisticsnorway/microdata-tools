import os
from pathlib import Path

import pytest

from microdata_tools.packaging.keys import PrivateKey


@pytest.fixture
def output_dir(tmp_path: Path) -> Path:
    return tmp_path / "output"


@pytest.fixture
def keys_dir(tmp_path: Path) -> Path:
    keys_dir = tmp_path / "keys_dir"
    generate_keys(keys_dir)
    return keys_dir


def generate_keys(keys_dir: Path) -> None:
    if not keys_dir.exists():
        os.makedirs(keys_dir)
    private_key = PrivateKey.generate()
    private_key.write_to_file(keys_dir)
    private_key.public_key().write_to_file(keys_dir)


def pytest_addoption(parser):
    parser.addoption(
        "--include-big-data",
        action="store_true",
        dest="include-big-data",
        default=False,
        help="enable big data testing",
    )
    parser.addoption(
        "--keep-big-data-csv-files",
        action="store_true",
        dest="keep-big-data-csv-files",
        default=False,
        help="Don't delete big data CSV files",
    )
    parser.addoption(
        "--big-data-row-count",
        action="store",
        dest="big-data-row-count",
        default="10_000_000",
        help="Number of rows to use for big datasets test",
    )


if "quiet" == os.environ.get("MICRODATA_TOOLS_TEST_PROGRESS"):

    def pytest_report_teststatus(report):
        category, short, verbose = "", "", ""
        if hasattr(report, "wasxfail"):
            if report.skipped:
                category = "xfailed"
                verbose = "xfail"
            elif report.passed:
                category = "xpassed"
                verbose = ("XPASS", {"yellow": True})
            return (category, short, verbose)
        elif report.when in ("setup", "teardown"):
            if report.failed:
                category = "error"
                verbose = "ERROR"
            elif report.skipped:
                category = "skipped"
                verbose = "SKIPPED"
            return (category, short, verbose)
        category = report.outcome
        verbose = category.upper()
        return (category, short, verbose)
