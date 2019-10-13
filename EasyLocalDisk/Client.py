import os

from EasyLocalDisk.ClientError import ClientError
from EasyLog.Log import Log


# noinspection PyBroadException
class Client:
    @staticmethod
    def path_create(path) -> None:
        """
        Create a path on the local filesystem if it does not already exist

        :type path: str
        :param path: The path to create

        :return: None
        """
        if Client.file_exists(filename=path) is False:
            # Path was not found, attempt to create it
            try:
                os.makedirs(path)
            except Exception as path_create_exception:
                # Something went wrong creating the path
                raise Exception(ClientError.ERROR_PATH_CREATE_UNHANDLED_EXCEPTION, path_create_exception)

            if Client.file_exists(filename=path) is False:
                # The path can't be found after we created it
                raise Exception(ClientError.ERROR_PATH_CREATE_FAILED)

    @staticmethod
    def file_exists(filename) -> bool:
        """
        Check if file exists on local disk

        :type filename: str
        :param filename: The full path/filename to check

        :return: bool
        """
        Log.trace("Checking Local File Exists: {filename}".format(filename=filename))
        try:
            return os.path.exists(filename)
        except Exception as exists_exception:
            Log.exception(ClientError.ERROR_FILE_EXISTS_UNHANDLED_EXCEPTION, exists_exception)

    @staticmethod
    def file_readable(filename) -> bool:
        """
        Check if the file is readable on local disk

        :type filename: str
        :param filename: The full path/filename to check

        :return: bool
        """
        Log.trace("Checking Local File Exists: {filename}".format(filename=filename))

        file_readable = None
        try:
            file = open(filename, "r")
            file_readable = file.readable()
            file.close()
        except Exception as readable_exception:
            Log.exception(ClientError.ERROR_FILE_READABLE_UNHANDLED_EXCEPTION, readable_exception)

        return file_readable

    @staticmethod
    def file_delete(filename) -> None:
        """
        Delete a file from local disk

        :type filename: str
        :param filename: Full path/filename to be deleted

        :return: None
        """
        Log.trace('Deleting Local File: {filename}'.format(filename=filename))

        Log.debug('Checking File Exists...')
        if Client.file_exists(filename=filename) is False:
            # Could not locate the file
            Log.exception(ClientError.ERROR_FILE_DELETE_NOT_FOUND)

        # Delete file we created
        try:
            Log.debug('Deleting file...')
            os.unlink(filename)
        except Exception as cleanup_exception:
            # Unable to remove the file
            Log.exception(ClientError.ERROR_FILE_DELETE_UNHANDLED_EXCEPTION, cleanup_exception)

    @staticmethod
    def file_create_from_string(filename, contents, allow_overwrite=True) -> None:
        """
        Create a new file on local disk from the contents of the supplied string

        :type filename: str
        :param filename:

        :type contents: str
        :param contents:

        :type allow_overwrite: bool
        :param allow_overwrite:

        :return: None
        """
        Log.trace('Creating Local File From String...')

        if allow_overwrite is False:
            Log.debug('Checking If File Already Exists...')
            if Client.file_exists(filename=filename) is True:
                Log.exception(ClientError.ERROR_FILE_CREATE_ALREADY_EXISTS)

        try:
            Log.debug('Creating New File...')
            file = open(filename, "w")
            file.write(contents)
            file.close()
        except Exception as write_exception:
            # Unexpected error during creation of file
            Log.exception(ClientError.ERROR_FILE_CREATE_UNHANDLED_EXCEPTION, write_exception)

        Log.debug('Checking New File Exists...')
        if Client.file_exists(filename=filename) is False:
            # Could not locate the new file
            Log.exception(ClientError.ERROR_FILE_CREATE_FAILED)

        Log.debug('Checking New File Readable...')
        if Client.file_readable(filename=filename) is False:
            # Could not locate read the new file
            Log.exception(ClientError.ERROR_FILE_CREATE_UNREADABLE)
