import pytest
import logging

LOGGER = logging.getLogger(__name__)


def test_is_unit():
    LOGGER.debug("This is unit test.")
    assert True
