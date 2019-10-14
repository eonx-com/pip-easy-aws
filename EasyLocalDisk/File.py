#!/usr/bin/env python
# -*- coding: utf-8 -*-


from EasyLocalDisk.Client import Client
from EasyLog.Log import Log


class File:
    ERROR_REPLACE_SAME_FILE = ''
    ERROR_REPLACE_SOURCE_NOT_FOUND = ''
    ERROR_REPLACE_SOURCE_UNREADABLE = ''

    def __init__(self, filename):
        """
        :type filename: str
        :param filename:
        """
        self.__filename__ = filename

    def replace(self, local_filename) -> None:
        """
        :type local_filename: str
        :param local_filename: The path/filename of the file on local disk that is to replace the current file

        :return: None
        """
        Log.trace('Replacing File...')

        Log.debug('Validating Source/Destination...')
        if local_filename == self.__filename__:
            Log.exception(File.ERROR_REPLACE_SAME_FILE)

        Log.debug('Validating Source Exists...')
        if Client.file_exists(filename=local_filename) is False:
            Log.exception(File.ERROR_REPLACE_SOURCE_NOT_FOUND)

        Log.debug('Validating Source Readable...')
        if Client.file_readable(filename=local_filename) is False:
            Log.exception(File.ERROR_REPLACE_SOURCE_UNREADABLE)

        Log.debug('Copying...')
        Client.file_copy(source_filename=local_filename, destination_filename=self.__filename__)


