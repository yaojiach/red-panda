import logging

LOGGER = logging.getLogger(__name__)


def test_integ_test(s3_bucket):
    LOGGER.info("This is integ test.")
    LOGGER.info(s3_bucket)
    assert True
