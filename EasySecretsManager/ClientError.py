#!/usr/bin/env python
# -*- coding: utf-8 -*-


class ClientError:
    # Secret List Errors
    ERROR_SECRET_LIST_UNHANDLED_EXCEPTION = 'An unexpected error occurred during listing of secrets'

    # Secret Create Errors
    ERROR_SECRET_CREATE_UNHANDLED_EXCEPTION = 'An unexpected error occurred during creation of secrets'
    ERROR_SECRET_CREATE_ALREADY_EXISTS = 'An unexpected error occurred during creation of secret, the specified secret already exists'

    # Secret Update Errors
    ERROR_SECRET_UPDATE_UNHANDLED_EXCEPTION = 'An unexpected error occurred during update of secrets'
    ERROR_SECRET_UPDATE_NOT_FOUND = 'An unexpected error occurred during update of secret, the requested secret could not be found'

    # Secret Delete Errors
    ERROR_SECRET_DELETE_UNHANDLED_EXCEPTION = 'An unexpected error occurred during deletion of secrets'
    ERROR_SECRET_DELETE_NOT_FOUND = 'An unexpected error occurred during delete of secret, the requested secret could not be found'
