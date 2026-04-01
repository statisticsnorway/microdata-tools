import logging

import pytest

logger = logging.getLogger()


@pytest.mark.focus
def test_sqlite_pagination():
    logger.info("janei")
