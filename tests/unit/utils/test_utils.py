import pytest
import logging

from red_panda.utils import filter_kwargs, prettify_sql, make_valid_uri

LOGGER = logging.getLogger(__name__)


def test_filter_kwargs():
    KWARGS = {"a": 1, "b": 2}
    FILTER = ["b"]
    FILTERED = {"b": 2}
    assert filter_kwargs(KWARGS, FILTER) == FILTERED


def test_prettify_sql_formats_properly():
    SQL = """\
    create table as

    select *
        from table
    iam_role '12345';
    """
    PRETTIFIED = "create table as\nselect *\nfrom table\niam_role '********';\n"
    assert prettify_sql(SQL) == PRETTIFIED


def test_make_valid_uri_raises_with_invalid_args():
    with pytest.raises(ValueError):
        make_valid_uri(["1"])


@pytest.mark.parametrize(
    "test_input", [["s3://", "bucket", "path"], ["s3://", "/bucket", "path"]]
)
def test_make_valid_uri(test_input):
    URI = "s3://bucket/path"
    assert make_valid_uri(*test_input) == URI
