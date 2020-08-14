import pytest
import logging

from red_panda.utils import filter_kwargs, make_valid_uri

LOGGER = logging.getLogger(__name__)


def test_filter_kwargs():
    KWARGS = {"a": 1, "b": 2}
    FILTER = ["b"]
    FILTERED = {"b": 2}
    assert filter_kwargs(KWARGS, FILTER) == FILTERED


def test_make_valid_uri_raises_with_invalid_args():
    with pytest.raises(ValueError):
        make_valid_uri(["1"])


def test_make_valid_uri():
    VALUES = ["s3://", "bucket", "path"]
    URI = "s3://bucket/path"
    assert make_valid_uri(*VALUES) == URI
