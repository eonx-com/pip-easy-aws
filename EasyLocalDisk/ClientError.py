#!/usr/bin/env python
# -*- coding: utf-8 -*-


class ClientError:
    # File Exists Errors
    ERROR_FILE_EXISTS_UNHANDLED_EXCEPTION = 'An unexpected error occurred during test of file existence on local filesystem'

    # File Readable Error
    ERROR_FILE_READABLE_UNHANDLED_EXCEPTION = 'An unexpected error occurred during test of file readability on local filesystem'

    # File Create Error
    ERROR_FILE_CREATE_UNHANDLED_EXCEPTION = ''
    ERROR_FILE_CREATE_ALREADY_EXISTS = 'An unexpected error occurred during creation of file. The requested file already exists and overwrite is disabled.'
    ERROR_FILE_CREATE_FAILED = 'An unexpected error occurred during creation of file, the newly created file could not be found'
    ERROR_FILE_CREATE_UNREADABLE = 'An unexpected error occurred during creation of file, the newly created file could not be read'

    ERROR_FILE_DELETE_UNHANDLED_EXCEPTION = ''
    ERROR_FILE_DELETE_NOT_FOUND = ''

    ERROR_PATH_CREATE_UNHANDLED_EXCEPTION = ''
    ERROR_PATH_CREATE_FAILED = ''

    ERROR_FILE_MOVE_ALREADY_EXISTS = ''
    ERROR_FILE_MOVE_FAILED = ''
