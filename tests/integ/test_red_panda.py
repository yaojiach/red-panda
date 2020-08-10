import pytest
import logging

LOGGER = logging.getLogger(__name__)


def test_integ_test():
    LOGGER.debug("This is integ test.")
    assert True
