#!/usr/bin/env python
# -*- coding: utf-8 -*-


class ClientError:
    # Bucket List Errors
    ERROR_BUCKET_LIST_UNHANDLED_EXCEPTION = 'An unexpected error occurred during listing of S3 buckets'
    ERROR_BUCKET_LIST_KEY_MISSING = 'An unexpected error occurred during list of S3 buckets, the result did not contain the expected key'

    # File List Errors
    ERROR_FILE_LIST_UNHANDLED_EXCEPTION = 'An unexpected error occurred during listing of files in S3 bucket'
    ERROR_FILE_LIST_KEY_NOT_FOUND = 'An unexpected error occurred during listing of files in S3, the result did not contain the expected key'

    # File Exists Errors
    ERROR_FILE_EXISTS_UNHANDLED_EXCEPTION = 'An unexpected error occurred during test of file existence in S3 bucket'

    # File Delete Errors
    ERROR_FILE_DELETE_UNHANDLED_EXCEPTION = 'An unexpected error occurred while deleting S3 file'
    ERROR_FILE_DELETE_SOURCE_NOT_FOUND = 'The file you are attempting to delete does not exist'
    ERROR_FILE_DELETE_FAILED = 'The file you are attempting to delete was not successfully deleted'

    # Read Tags Errors
    ERROR_FILE_TAG_READ_SOURCE_NOT_FOUND = 'The file you are attempting to delete does not exist'
    ERROR_FILE_TAG_READ_EXCEPTION = 'An unexpected error occurred while attempting to retrieve tags from file in S3'

    # Write Tags Errors
    ERROR_FILE_TAG_WRITE_EXCEPTION = 'An unexpected error occurred while attempting to update tags on file in S3'
    ERROR_FILE_TAG_WRITE_FAILED = 'An unexpected error occurred while attempting to update the file tags, the retrieved tags did not match those that were saved'

    # File Copy Errors
    ERROR_FILE_COPY_UNHANDLED_EXCEPTION = 'An unexpected error occurred while copying S3 file'
    ERROR_FILE_COPY_SOURCE_DESTINATION_SAME = 'The source and destination of the file copy cannot be the same'
    ERROR_FILE_COPY_SOURCE_NOT_FOUND = 'The requested file did not exist in the source bucket'
    ERROR_FILE_COPY_DESTINATION_EXISTS = 'The requested file already exists in the destination bucket'
    ERROR_FILE_COPY_FAILED = 'The requested file could not be found in the destination bucket after the file copy'

    # File Move Errors
    ERROR_FILE_MOVE_UNHANDLED_EXCEPTION = 'An unexpected error occurred while moving S3 file'
    ERROR_FILE_MOVE_COPY_EXCEPTION = 'An unexpected error occurred while moving S3 file, copying to the destination failed'
    ERROR_FILE_MOVE_DELETE_EXCEPTION = 'An unexpected error occurred while moving S3 file, deleting original file after copy failed'

    # File Upload Errors
    ERROR_FILE_UPLOAD_UNHANDLED_EXCEPTION = 'An unexpected error occurred while uploading file to S3'
    ERROR_FILE_UPLOAD_SOURCE_NOT_FOUND = 'The file was not found on the local filesystem'
    ERROR_FILE_UPLOAD_SOURCE_UNREADABLE = 'The file was found on the local filesystem however its content were not readable, please check file permissions'
    ERROR_FILE_UPLOAD_DESTINATION_EXISTS = 'The file upload failed as the file already exists and allow overwrite was not enabled'
    ERROR_FILE_UPLOAD_EXISTS = 'The file upload failed as the file already exists and allow overwrite was not enabled'
    ERROR_FILE_UPLOAD_SOURCE_TO_ARRAY_ERROR = "An unexpected error occurred while converting the supplied string to a byte array, please check the selected encoding"
    ERROR_FILE_UPLOAD_SOURCE_TO_FILE_OBJECT_ERROR = "An unexpected error occurred while converting the supplied string to a file object"

    # File Download Errors
    ERROR_FILE_DOWNLOAD_UNHANDLED_EXCEPTION = 'An unexpected error occurred while downloading file from S3'
    ERROR_FILE_DOWNLOAD_DESTINATION_EXISTS = 'The file download failed as the file already exists and allow overwrite was not enabled'
    ERROR_FILE_DOWNLOAD_SOURCE_NOT_FOUND = 'The file download failed, the file could not be found in the destination folder'
    ERROR_FILE_DOWNLOAD_DESTINATION_NOT_FOUND = 'The file download failed, the file could not be found in the destination folder'
    ERROR_FILE_DOWNLOAD_BODY_EXCEPTION = 'An unexpected error occurred while attempting to retrieve downloaded file body'
    ERROR_FILE_DOWNLOAD_READ_EXCEPTION = 'An unexpected error occurred while attempting to read downloaded file body'
    ERROR_FILE_DOWNLOAD_DECODE_EXCEPTION = 'An unexpected error occurred while attempting to decode the downloaded file to a string'
    ERROR_FILE_DOWNLOAD_DESTINATION_NOT_READABLE = 'An unexpected error occurred while attempting to download the file, the downloaded file was not readable'
