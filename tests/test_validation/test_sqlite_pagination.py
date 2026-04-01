import logging

import pytest

logger = logging.getLogger()


@pytest.mark.focus
def janei():
    logger.info("janei")
