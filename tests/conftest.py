import os


def pytest_addoption(parser):
    parser.addoption(
        "--include-big-data",
        action="store_true",
        dest="include-big-data",
        default=False,
        help="enable big data testing",
    )


if "true" == os.environ.get("MICRODATA_TOOLS_WATCH_MODE"):

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
