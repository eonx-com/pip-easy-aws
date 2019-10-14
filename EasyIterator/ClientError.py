#!/usr/bin/env python
# -*- coding: utf-8 -*-

class ClientError:
    # Iterator Errors
    ERROR_SOURCE_LIST_EMPTY = 'The iterator was supplied an empty source list, please ensure at least one file source is provided'
    ERROR_SOURCE_CONFIG_INVALID = 'The configuration of one or more iterator sources was invalid'
    ERROR_SOURCE_TYPE_INVALID = 'Unknown file type was specified, expected S3 or SFTP'
