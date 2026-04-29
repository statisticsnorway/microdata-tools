import logging
import os
import re

import matplotlib.pyplot as plt
import pytest

from tests import log_setup

logger = logging.getLogger()


def mem_log_file():
    k = "MICRODATA_TOOLS_MEM_LOG_FILE"
    assert k in os.environ
    return os.getenv(k)


def parse_data(log_file):
    rows = []
    with open(log_file) as fd:
        t = -1
        for idx, lin in enumerate(fd.readlines()):
            lin = lin.rstrip()
            if lin == "" or idx == 0:
                continue
            if idx == 2:
                v = (
                    "     pgpgin/s pgpgout/s   fault/s  majflt/s  pgfree/s "
                    "pgscank/s pgscand/s pgsteal/s  pgprom/s   pgdem/s"
                )
                assert lin.endswith(v)
                t = 0
                continue

            t += 1
            parts = re.split(r"\s+", lin)

            row = {
                "time": t,
                "pgpin/s": float(parts[1]),
                "pgpout/s": float(parts[2]),
                "faults/s": float(parts[3]),
                "majflt/s": float(parts[4]),
                "pgfree/s": float(parts[5]),
                "pgscank/s": float(parts[6]),
                "pgscand/s": float(parts[7]),
                "pgsteal/s": float(parts[8]),
                "pgprom/s": float(parts[9]),
                "pgdem/s": float(parts[10]),
            }

            rows.append(row)
    return rows


@pytest.mark.mem_viz
def test_mem_viz():
    log_setup.init_logging()
    data = parse_data(mem_log_file())

    x = [row["time"] for row in data]
    y_label = "majflt/s"
    y = [row[y_label] for row in data]
    plt.plot(x, y)
    plt.ylabel(y_label)
    plt.xlabel("time (seconds)")
    plt.savefig("plot.png")
    print("done!")
