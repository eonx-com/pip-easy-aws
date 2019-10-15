#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import tempfile
import uuid

from EasyLocalDisk.ClientError import ClientError
from EasyLog.Log import Log
from shutil import copyfile


class Client:
    # Baseline file handling functions

    @staticmethod
    def create_path(path, allow_overwrite=False) -> None:
        """
        Create path in the sftp_filesystem

        :type path: str
        :param path: The path to create

        :type allow_overwrite: bool
        :param allow_overwrite: Flag indicating the path is allowed to be overwritten if it already exists. If False, and the path exists an exception will be thrown

        :return: None
        """
        # Sanitize the path
        path = Client.sanitize_path(path)

        if Client.file_exists(filename=path) is False:
            # Path was not found, attempt to create it
            try:
                os.makedirs(path, exist_ok=allow_overwrite)
            except Exception as create_path_exception:
                Log.exception(ClientError.ERROR_CREATE_PATH_UNHANDLED_EXCEPTION, create_path_exception)

            # Make sure creation was successful
            if Client.file_exists(filename=path) is False:
                Log.exception(ClientError.ERROR_CREATE_PATH_FAILED)
        elif allow_overwrite is False:
            # Path already existed, an overwrite was False
            Log.exception(ClientError.ERROR_CREATE_PATH_ALREADY_EXISTS)

    @staticmethod
    def create_temp_path(prefix='', temp_path=None) -> str:
        """
        Create a new local path inside the system temp folder that is guaranteed to be unique

        :type prefix: str
        :param prefix: Optional prefix to prepend to path

        :type temp_path: str
        :param temp_path: The base path for all temporary files (if not supplied system temp folder will be used)

        :return: str
        """
        # If no temporary folder was specified, use the system temp folder
        if temp_path is None:
            temp_path = tempfile.gettempdir()

        # Sanitize the supplied path
        temp_path = Client.sanitize_path(temp_path)

        # Make sure the temporary folder exists
        if Client.file_exists(temp_path) is False:
            Log.exception(ClientError.ERROR_CREATE_TEMP_PATH_FOLDER_NOT_FOUND)

        # Enter a loop until we find a folder name that doesn't already exist
        count_attempts = 0
        while True:
            count_attempts += 1
            local_path = '{temp_path}{prefix}{uuid}'.format(temp_path=temp_path, prefix=prefix, uuid=uuid.uuid4())
            if os.path.exists(local_path) is False:
                # noinspection PyBroadException
                try:
                    os.makedirs(local_path, exist_ok=False)
                    return Client.sanitize_path(local_path)
                except Exception:
                    # If creation fails its likely because it exists- so we will just try again next time
                    pass
            if count_attempts > 10:
                # If after 100 attempts we haven't found a unique path, something is obviously wrong so we need to fail
                Log.exception(ClientError.ERROR_CREATE_TEMP_PATH_FAILED)

    @staticmethod
    def file_list(path, recursive=False) -> list:
        """
        List all files in directory

        :return list[str]:
        """
        # Sanitize the path
        path = Client.sanitize_path(path)

        files = []

        try:
            # Walk the folder structures
            for root, directories, files in os.walk(path):
                for file in files:
                    files.append(os.path.join(root, file))

                # If not recursive, break out after the initial path is listed
                if recursive is False:
                    break
        except Exception as list_exception:
            Log.exception(ClientError.ERROR_FILE_LIST_UNHANDLED_EXCEPTION, list_exception)

        return files

    @staticmethod
    def file_exists(filename) -> bool:
        """
        Check if file exists on local disk

        :type filename: str
        :param filename: The full path/filename to check

        :return: bool
        """
        # Sanitize the filename
        filename = Client.sanitize_filename(filename)

        try:
            return os.path.exists(filename)
        except Exception as exists_exception:
            Log.exception(ClientError.ERROR_FILE_EXISTS_UNHANDLED_EXCEPTION, exists_exception)

    @staticmethod
    def file_delete(filename, allow_missing=False) -> None:
        """
        Delete a file from local disk

        :type filename: str
        :param filename: Full path/filename to be deleted

        :type allow_missing: bool
        :param allow_missing: Boolean flag, if False and the file cannot be found to delete an exception will be raised

        :return: None
        """
        # Sanitize the filename
        filename = Client.sanitize_filename(filename)

        # Make sure the file exists
        if Client.file_exists(filename) is False:
            # If this is fine, get out of here- the file is already gone
            if allow_missing is True:
                return
            Log.exception(ClientError.ERROR_FILE_DELETE_NOT_FOUND)

        # Delete the file
        try:
            os.unlink(filename)
        except Exception as delete_exception:
            Log.exception(ClientError.ERROR_FILE_DELETE_UNHANDLED_EXCEPTION, delete_exception)

        # Make sure the file no longer exists after deleting
        if Client.file_exists(filename) is True:
            Log.exception(ClientError.ERROR_FILE_DELETE_FAILED)

    @staticmethod
    def file_move(source_filename, destination_filename, allow_overwrite=True) -> None:
        """
        Move/rename a file

        :type source_filename: str
        :param source_filename:

        :type destination_filename: str
        :param destination_filename:

        :type allow_overwrite: bool
        :param allow_overwrite:

        :return: None
        """
        # Sanitize the filenames
        source_filename = Client.sanitize_filename(source_filename)
        destination_filename = Client.sanitize_filename(destination_filename)

        # Ensure the source and destination are not the same
        if source_filename == destination_filename:
            Log.exception(ClientError.ERROR_FILE_MOVE_SOURCE_DESTINATION_SAME)

        # If overwrite is disabled, make sure the destination file does not already exist
        if allow_overwrite is False:
            if Client.file_exists(filename=destination_filename) is True:
                Log.exception(ClientError.ERROR_FILE_MOVE_ALREADY_EXISTS)

        # Make sure the source file exists
        if Client.file_exists(source_filename) is False:
            Log.exception(ClientError.ERROR_FILE_MOVE_SOURCE_NOT_FOUND)

        # Move the file
        try:
            os.replace(source_filename, destination_filename)
        except Exception as move_exception:
            Log.exception(ClientError.ERROR_FILE_MOVE_UNHANDLED_EXCEPTION, move_exception)

        # Make sure the file moved to the destination
        if Client.file_exists(destination_filename) is False:
            Log.exception(ClientError.ERROR_FILE_MOVE_COPY_FAILED)

        # Make sure the source no longer exists
        if Client.file_exists(source_filename) is True:
            Log.exception(ClientError.ERROR_FILE_MOVE_DELETE_FAILED)

    @staticmethod
    def file_copy(source_filename, destination_filename, allow_overwrite=True) -> None:
        """
        Copy a file

        :type source_filename: str
        :param source_filename:

        :type destination_filename: str
        :param destination_filename:

        :type allow_overwrite: bool
        :param allow_overwrite:

        :return: None
        """
        # Sanitize the filenames
        source_filename = Client.sanitize_filename(source_filename)
        destination_filename = Client.sanitize_filename(destination_filename)

        # If overwrite is disabled, make sure the destination file does not already exist
        if allow_overwrite is False:
            if Client.file_exists(filename=destination_filename) is True:
                Log.exception(ClientError.ERROR_FILE_COPY_ALREADY_EXISTS)

        try:
            copyfile(source_filename, destination_filename)
        except Exception as copy_exception:
            Log.exception(ClientError.ERROR_FILE_COPY_UNHANDLED_EXCEPTION, copy_exception)

        # Make sure the file copied to the destination
        if Client.file_exists(destination_filename) is False:
            Log.exception(ClientError.ERROR_FILE_COPY_FAILED)

    @staticmethod
    def sanitize_path(path) -> str:
        """
        Sanitize a path

        :type path: str
        :param path: The path to sanitize

        :return: str
        """
        # Remove all leading/trailing whitespace
        path = str(path).strip()

        # Fix directory separators
        if os.sep == '/':
            path = path.replace('\\', '/')
        elif os.sep == '\\':
            path = path.replace('/', '\\')

        # Remove all duplicated directory separators slashes in the pat
        while '{separator}{separator}'.format(separator=os.sep) in path:
            path = path.replace('{separator}{separator}'.format(separator=os.sep), os.sep)

        # Strip all leading/trailing directory separators
        path = path.strip(os.sep)

        # Add directory separator to start and end of string before returning
        return '{separator}{path}{separator}'.format(path=path, separator=os.sep)

    @staticmethod
    def sanitize_filename(filename) -> str:
        """
        Sanitize a full path/filename

        :type filename: str
        :param filename: Path/Filename to sanitize

        :return: str
        """
        # Make sure we got a string
        filename = str(filename)

        # Sanitize the path component of the supplied string
        path = Client.sanitize_path(os.path.dirname(filename))

        # Extract the filename
        filename = os.path.basename(filename)

        # Concatenate them together
        return '{path}{filename}'.format(path=path, filename=filename)

    # Local sftp_filesystem specific

    @staticmethod
    def is_file_readable(filename) -> bool:
        """
        Check if the file is readable on local sftp_filesystem

        :type filename: str
        :param filename: The full path/filename to check

        :return: bool
        """
        # Sanitize the filename
        filename = Client.sanitize_filename(filename)

        # Test file can be read using local sftp_filesystem
        file_readable = False
        try:
            file = open(filename, "r")
            file_readable = file.readable()
            file.close()
        except Exception as readable_exception:
            Log.exception(ClientError.ERROR_FILE_READABLE_UNHANDLED_EXCEPTION, readable_exception)

        return file_readable
