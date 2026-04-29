import logging
import os

import pytest

from tests import log_setup

logger = logging.getLogger()


def mem_log_file():
    k = "MICRODATA_TOOLS_MEM_LOG_FILE"
    assert k in os.environ
    return os.getenv(k)


@pytest.mark.mem_viz
def test_mem_viz():
    log_setup.init_logging()

    with open(mem_log_file()) as fd:
        for lin in fd.readlines():
            lin = lin.rstrip()
            print(lin)
