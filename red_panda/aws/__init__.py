REDSHIFT_RESERVED_WORDS = [
    "aes128",
    "aes256",
    "all",
    "allowoverwrite",
    "analyse",
    "analyze",
    "and",
    "any",
    "array",
    "as",
    "asc",
    "authorization",
    "backup",
    "between",
    "binary",
    "blanksasnull",
    "both",
    "bytedict",
    "bzip2",
    "case",
    "cast",
    "check",
    "collate",
    "column",
    "constraint",
    "create",
    "credentials",
    "cross",
    "current_date",
    "current_time",
    "current_timestamp",
    "current_user",
    "current_user_id",
    "default",
    "deferrable",
    "deflate",
    "defrag",
    "delta",
    "delta32k",
    "desc",
    "disable",
    "distinct",
    "do",
    "else",
    "emptyasnull",
    "enable",
    "encode",
    "encrypt     ",
    "encryption",
    "end",
    "except",
    "explicit",
    "false",
    "for",
    "foreign",
    "freeze",
    "from",
    "full",
    "globaldict256",
    "globaldict64k",
    "grant",
    "group",
    "gzip",
    "having",
    "identity",
    "ignore",
    "ilike",
    "in",
    "initially",
    "inner",
    "intersect",
    "into",
    "is",
    "isnull",
    "join",
    "leading",
    "left",
    "like",
    "limit",
    "localtime",
    "localtimestamp",
    "lun",
    "luns",
    "lzo",
    "lzop",
    "minus",
    "mostly13",
    "mostly32",
    "mostly8",
    "natural",
    "new",
    "not",
    "notnull",
    "null",
    "nulls",
    "off",
    "offline",
    "offset",
    "oid",
    "old",
    "on",
    "only",
    "open",
    "or",
    "order",
    "outer",
    "overlaps",
    "parallel",
    "partition",
    "percent",
    "permissions",
    "placing",
    "primary",
    "raw",
    "readratio",
    "recover",
    "references",
    "respect",
    "rejectlog",
    "resort",
    "restore",
    "right",
    "select",
    "session_user",
    "similar",
    "snapshot ",
    "some",
    "sysdate",
    "system",
    "table",
    "tag",
    "tdes",
    "text255",
    "text32k",
    "then",
    "timestamp",
    "to",
    "top",
    "trailing",
    "true",
    "truncatecolumns",
    "union",
    "unique",
    "user",
    "using",
    "verbose",
    "wallet",
    "when",
    "where",
    "with",
    "without",
]

REDSHIFT_COPY_KWARGS = [
    "delimiter",
    "ignoreheader",
    "quote_character",
    "dateformat",
    "timeformat",
    "region",
    "null",
    "escape",  # bool
    "acceptinvchars",  # str
    "iam_role",
    "acceptanydate",  # bool
    "column_list",  # list[str]
    "blanksasnull",  # bool
    "emptyasnull",  # bool
    "encoding",  # str
    "explicit_ids",  # bool
    "fillrecord",  # bool
    "ignoreblanklines",  # bool
    "removequotes",  # bool
    "roundec",  # bool
    "trimblanks",  # bool
    "truncatecolumns",  # bool
]

REDSHIFT_UNLOAD_KWARGS = [
    "manifest",  # bool
    "delimiter",
    "fixedwidth",
    "encrypted",  # bool
    "bzip2",  # bool
    "gzip",  # bool
    "addquotes",  # bool
    "null",
    "escap",  # bool
    "allowoverwrite",  # bool
    "parallel",
    "maxfilesize",
]

S3_PUT_KWARGS = [
    "ACL",
    # 'Body', required in our context and handled seperately
    "CacheControl",
    "ContentDisposition",
    "ContentEncoding",
    "ContentLanguage",
    "ContentLength",
    "ContentMD5",
    "ContentType",
    "Expires",
    "GrantFullControl",
    "GrantRead",
    "GrantReadACP",
    "GrantWriteACP",
    # 'Key', required and handled seperately
    "Metadata",
    "ServerSideEncryption",
    "StorageClass",
    "WebsiteRedirectLocation",
    "SSECustomerAlgorithm",
    "SSECustomerKey",
    "SSEKMSKeyId",
    "RequestPayer",
    "Tagging",
]

S3_GET_KWARGS = [
    "Bucket",
    "IfMatch",
    "IfModifiedSince",
    "IfNoneMatch",
    "IfUnmodifiedSince",
    "Key",
    "Range",
    "ResponseCacheControl",
    "ResponseContentDisposition",
    "ResponseContentEncoding",
    "ResponseContentLanguage",
    "ResponseContentType",
    "ResponseExpires",
    "VersionId",
    "SSECustomerAlgorithm",
    "SSECustomerKey",
    "RequestPayer",
    "PartNumber",
]

S3_CREATE_BUCKET_KWARGS = [
    "ACL",
    "CreateBucketConfiguration",
    "GrantFullControl",
    "GrantRead",
    "GrantReadACP",
    "GrantWrite",
    "GrantWriteACP",
]


class AWSUtils:
    """ Base class for AWS operations.
    
    Args:
        aws_config: AWS configuration.

    Attributes:
        aws_config (dict): AWS configuration.
    """

    def __init__(self, aws_config: dict):
        if aws_config is None:
            aws_config = {
                "aws_access_key_id": None,
                "aws_secret_access_key": None,
                "aws_session_token": None,
                "region_name": None,
            }
        self.aws_config = aws_config
