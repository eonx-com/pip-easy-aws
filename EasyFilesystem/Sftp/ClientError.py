#!/usr/bin/env python
# -*- coding: utf-8 -*-

class ClientError:
    # File List Errors
    ERROR_FILE_LIST_UNHANDLED_EXCEPTION = 'An unexpected exception error occurred while listing files'

    # Make Directory Errors
    ERROR_MAKE_PATH_FAILED = 'An unexpected exception error occurred while attempting to ensure directory exists'

    # File Exists Errors
    ERROR_FILE_EXISTS_UNHANDLED_EXCEPTION = 'An unexpected error occurred during test of file existence on SFTP server'

    ERROR_CONNECT_FAILED = 'Failed to connect to SFTP server'
    ERROR_CONNECT_INVALID_FINGERPRINT = 'Failed to connect to SFTP server, invalid server fingerprint'

    ERROR_INVALID_FINGERPRINT = 'Failed to validate the SFTP server host key/fingerprint'
    ERROR_INVALID_FINGERPRINT_TYPE = 'The specified sftp_fingerprint type was not known'

    ERROR_FILE_DELETE_UNHANDLED_EXCEPTION = 'An unexpected error occurred while deleting S3 file'
    ERROR_FILE_DELETE_NOT_FOUND = 'The file you are attempting to delete does not exist'
    ERROR_FILE_DELETE_FAILED = 'The file you are attempting to delete was not successfully deleted'

    # File Download Errors
    ERROR_FILE_DOWNLOAD_UNHANDLED_EXCEPTION = 'An unexpected error occurred while downloading file from S3'
    ERROR_FILE_DOWNLOAD_ALREADY_EXISTS = 'The file download failed as the file already exists and allow overwrite was not enabled'
    ERROR_FILE_DOWNLOAD_SOURCE_NOT_FOUND = 'The file download failed, the file could not be found in the destination folder'
    ERROR_FILE_DOWNLOAD_DESTINATION_NOT_FOUND = 'The file download failed, the file could not be found in the destination folder'
    ERROR_FILE_DOWNLOAD_BODY_EXCEPTION = 'An unexpected error occurred while attempting to retrieve downloaded file body'
    ERROR_FILE_DOWNLOAD_READ_EXCEPTION = 'An unexpected error occurred while attempting to read downloaded file body'
    ERROR_FILE_DOWNLOAD_DECODE_EXCEPTION = 'An unexpected error occurred while attempting to decode the downloaded file to a string'
    ERROR_FILE_DOWNLOAD_DESTINATION_NOT_READABLE = 'An unexpected error occurred while attempting to download the file, the downloaded file was not readable'
    ERROR_FILE_DOWNLOAD_CALLBACK_NOT_CALLABLE = ''
    ERROR_FILE_DOWNLOAD_FAILED = ''

    # File Upload Errors
    ERROR_FILE_UPLOAD_UNHANDLED_EXCEPTION = 'An unexpected exception error occurred during file upload'
    ERROR_FILE_UPLOAD_CREATE_PATH = 'An unexpected exception error occurred during file upload, failed to create destination folder'
    ERROR_FILE_UPLOAD_EXISTS = 'The file upload failed as the file already exists and allow overwrite was not enabled'
    ERROR_FILE_UPLOAD_TEMP_CREATE = 'An unexpected exception error occurred during file upload, unable to create temporary file'
    ERROR_FILE_UPLOAD_TEMP_READABLE = 'An unexpected exception error occurred during file upload, unable to validate temporary file is readable'
    ERROR_FILE_UPLOAD_TEMP_CLEANUP = 'An unexpected exception error occurred during file upload, unable to delete temporary file'
    ERROR_FILE_UPLOAD_CALLBACK = 'An unexpected exception error occurred during file upload, user callback function generated an exception'
    ERROR_FILE_UPLOAD_TEMP_FOLDER = 'An unexpected exception error occurred during file upload, unable to create temporary folder'
    ERROR_FILE_UPLOAD_CALLBACK_NOT_CALLABLE = ''
    ERROR_FILE_UPLOAD_SOURCE_NOT_FOUND = ''
    ERROR_FILE_UPLOAD_ALREADY_EXISTS = ''
    ERROR_FILE_UPLOAD_FAILED = ''

    ERROR_FILE_MOVE_SOURCE_NOT_FOUND = ''
    ERROR_FILE_MOVE_DESTINATION_EXISTS = ''
    ERROR_FILE_MOVE_ALREADY_EXISTS = ''
    ERROR_FILE_MOVE_FAILED = ''
    ERROR_FILE_MOVE_SOURCE_DESTINATION_SAME = ''
    ERROR_FILE_MOVE_UNHANDLED_EXCEPTION = ''
    ERROR_FILE_MOVE_DELETE_FAILED = ''
    ERROR_FILE_MOVE_COPY_FAILED = ''

    ERROR_FILE_COPY_ALREADY_EXISTS = ''
    ERROR_FILE_COPY_CALLBACK_NOT_CALLABLE = ''
    ERROR_FILE_COPY_UNHANDLED_EXCEPTION = ''
    ERROR_FILE_COPY_SOURCE_NOT_FOUND = ''
    ERROR_FILE_COPY_SOURCE_DESTINATION_SAME = ''
    ERROR_FILE_COPY_FAILED = ''

    ERROR_CREATE_PATH_UNHANDLED_EXCEPTION = ''
    ERROR_CREATE_PATH_FAILED = ''
    ERROR_CREATE_PATH_ALREADY_EXISTS = ''

    ERROR_CREATE_TEMP_PATH_FOLDER_NOT_FOUND = ''
    ERROR_CREATE_TEMP_PATH_FAILED = ''

    ERROR_SANITIZE_ADDRESS = ''
    ERROR_SANITIZE_FINGERPRINT = ''
    ERROR_SANITIZE_PRIVATE_KEY = ''
    ERROR_SANITIZE_FINGERPRINT_TYPE = ''
    ERROR_SANITIZE_PORT = ''
    ERROR_SANITIZE_USERNAME = ''