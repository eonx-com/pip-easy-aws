#!/usr/bin/env python
# -*- coding: utf-8 -*-


class ClientError:
    ERROR_UNHANDLED_EXCEPTION = ' Please review additional error messages.'

    # Bucket List Errors
    ERROR_BUCKET_LIST = 'An unexpected error occurred during listing of available S3 buckets.'
    ERROR_BUCKET_LIST_UNHANDLED_EXCEPTION = ERROR_BUCKET_LIST + ERROR_UNHANDLED_EXCEPTION
    ERROR_BUCKET_LIST_INVALID_RESULT = ERROR_BUCKET_LIST + ' The result did not contain one or more expected keys.'

    # File List Errors
    ERROR_FILE_LIST = 'An unexpected error occurred during listing of files in S3 bucket.'
    ERROR_FILE_LIST_UNHANDLED_EXCEPTION = ERROR_FILE_LIST + ERROR_UNHANDLED_EXCEPTION
    ERROR_FILE_LIST_INVALID_RESULT = ERROR_FILE_LIST + ' The result did not contain one or more expected keys.'

    # File Exists Errors
    ERROR_FILE_EXISTS = 'An unexpected error occurred during test of file existence in S3 bucket.'
    ERROR_FILE_EXISTS_UNHANDLED_EXCEPTION = ERROR_FILE_EXISTS

    # File Delete Errors
    ERROR_FILE_DELETE = 'An unexpected error occurred while deleting S3 file.'
    ERROR_FILE_DELETE_UNHANDLED_EXCEPTION = ERROR_FILE_DELETE + ERROR_UNHANDLED_EXCEPTION
    ERROR_FILE_DELETE_FAILED = ERROR_FILE_DELETE + ' The deleted file still exists.'
    ERROR_FILE_DELETE_NOT_FOUND = ERROR_FILE_DELETE + ' The file to delete could not be found.'

    # Read Tags Errors
    ERROR_FILE_GET_TAGS = 'An unexpected error occurred while reading S3 file tags.'
    ERROR_FILE_GET_TAGS_UNHANDLED_EXCEPTION = ERROR_FILE_GET_TAGS + ERROR_UNHANDLED_EXCEPTION
    ERROR_FILE_GET_TAGS_SOURCE_NOT_FOUND = ERROR_FILE_GET_TAGS + ' The file does not exist.'

    # Write Tags Errors
    ERROR_FILE_PUT_TAGS = 'An unexpected error occurred while reading S3 file tags.'
    ERROR_FILE_PUT_TAGS_UNHANDLED_EXCEPTION = ERROR_FILE_PUT_TAGS + ERROR_UNHANDLED_EXCEPTION
    ERROR_FILE_PUT_TAGS_FAILED = ERROR_FILE_PUT_TAGS + ' Failed to write the requested tags.'

    # File Copy Errors
    ERROR_FILE_COPY = 'An unexpected error occurred while copying S3 file.'
    ERROR_FILE_COPY_UNHANDLED_EXCEPTION = ERROR_FILE_COPY + ERROR_UNHANDLED_EXCEPTION
    ERROR_FILE_COPY_FAILED = ERROR_FILE_COPY + ' Failed to copy the requested file.'
    ERROR_FILE_COPY_SOURCE_DESTINATION_SAME = ERROR_FILE_COPY + ' The source and destination of the file copy cannot be the same.'
    ERROR_FILE_COPY_SOURCE_NOT_FOUND = ERROR_FILE_COPY + ' The requested file did not exist in the source bucket.'
    ERROR_FILE_COPY_ALREADY_EXISTS = ERROR_FILE_COPY + ' The requested file already exists in the destination bucket.'

    # File Move Errors
    ERROR_FILE_MOVE = 'An unexpected error occurred while moving S3 file.'
    ERROR_FILE_MOVE_COPY_UNHANDLED_EXCEPTION = ERROR_FILE_MOVE + ' The copy to the destination failed. ' + ERROR_UNHANDLED_EXCEPTION
    ERROR_FILE_MOVE_DELETE_UNHANDLED_EXCEPTION = ERROR_FILE_MOVE + ' Deletion of the source file failed. ' + ERROR_UNHANDLED_EXCEPTION
    ERROR_FILE_MOVE_COPY_FAILED = ERROR_FILE_MOVE + ' Copying to the destination failed.'
    ERROR_FILE_MOVE_SOURCE_NOT_FOUND = ERROR_FILE_MOVE + ' The source file could not be found.'
    ERROR_FILE_MOVE_ALREADY_EXISTS = ERROR_FILE_MOVE + ' The destination file already exists.'
    ERROR_FILE_MOVE_DELETE_FAILED = ERROR_FILE_MOVE + ' Deleting the source file failed.'
    ERROR_FILE_MOVE_SOURCE_DESTINATION_SAME = ERROR_FILE_MOVE + ' The source and destination of the file move cannot be the same.'

    # File Upload Errors
    ERROR_FILE_UPLOAD = 'An unexpected error occurred while uploading file to S3.'
    ERROR_FILE_UPLOAD_UNHANDLED_EXCEPTION = ERROR_FILE_UPLOAD + ERROR_UNHANDLED_EXCEPTION
    ERROR_FILE_UPLOAD_SOURCE_NOT_FOUND = ERROR_FILE_UPLOAD + ' The source file could not be found.'
    ERROR_FILE_UPLOAD_ALREADY_EXISTS = ERROR_FILE_UPLOAD + ' The destination file already exists.'
    ERROR_FILE_UPLOAD_FAILED = ERROR_FILE_UPLOAD + ' The upload failed.'

    # File Download Errors
    ERROR_FILE_DOWNLOAD = 'An unexpected error occurred while download a file from S3.'
    ERROR_FILE_DOWNLOAD_UNHANDLED_EXCEPTION = ERROR_FILE_DOWNLOAD + ERROR_UNHANDLED_EXCEPTION
    ERROR_FILE_DOWNLOAD_SOURCE_NOT_FOUND = ERROR_FILE_DOWNLOAD + ' The source file could not be found.'
    ERROR_FILE_DOWNLOAD_CALLBACK_NOT_CALLABLE = ERROR_FILE_DOWNLOAD + ' The callback function was not a callable object.'
    ERROR_FILE_DOWNLOAD_ALREADY_EXISTS = ERROR_FILE_DOWNLOAD + ' The destination file already exists.'
    ERROR_FILE_DOWNLOAD_FAILED = ERROR_FILE_DOWNLOAD + ' The download failed.'

    # Create Path Errors
    ERROR_CREATE_PATH = 'An unexpected error occurred while attempting to create a path in S3.'
    ERROR_CREATE_PATH_UNHANDLED_EXCEPTION = ERROR_CREATE_PATH + ERROR_UNHANDLED_EXCEPTION
    ERROR_CREATE_PATH_ALREADY_EXISTS = ERROR_CREATE_PATH + ' The path already exists.'
    ERROR_CREATE_PATH_FAILED = ERROR_CREATE_PATH + ' Failed to create the requested path.'

    # Create Temp Path Errors
    ERROR_CREATE_TEMP_PATH = 'An unexpected error occurred while attempting to create a unique temp path in S3.'
    ERROR_CREATE_TEMP_PATH_FOLDER_NOT_FOUND = ERROR_CREATE_PATH + ' The base temp folder path specified did not exist.'
    ERROR_CREATE_TEMP_PATH_FAILED = ERROR_CREATE_PATH + ' Failed to create a unique temporary path.'
