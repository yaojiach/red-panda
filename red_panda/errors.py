# -*- coding: utf-8 -*-
class ReservedWordError(Exception):
    pass

class S3BucketExists(Exception):
    pass

class S3BucketNotExist(Exception):
    pass

class S3KeyNotExist(Exception):
    pass
