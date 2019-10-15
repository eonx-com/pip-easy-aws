#!/usr/bin/env python
# -*- coding: utf-8 -*-

from EasyFilesystem.Sftp.Server import Server as Server
from EasyLocalDisk.Client import Client as LocalDiskClient
from EasyLog.Log import Log


# noinspection DuplicatedCode
class File:
    ERROR_REPLACE_SAME_FILE = ''
    ERROR_REPLACE_SOURCE_NOT_FOUND = ''
    ERROR_REPLACE_SOURCE_UNREADABLE = ''

    def __init__(self, sftp_server, filename):
        """
        :type sftp_server: Server
        :param sftp_server: The server on which the file is located

        :type filename: str
        :param filename: The path/filename
        """
        self.__sftp_server__ = sftp_server
        self.__filename__ = filename

    def replace(self, local_filename) -> None:
        """
        :type local_filename: str
        :param local_filename:

        :return: None
        """
        Log.trace('Replacing File...')

        Log.debug('Validating Source Exists...')
        if LocalDiskClient.file_exists(filename=local_filename) is False:
            Log.exception(File.ERROR_REPLACE_SOURCE_NOT_FOUND)

        Log.debug('Validating Source Readable...')
        if LocalDiskClient.is_file_readable(filename=local_filename) is False:
            Log.exception(File.ERROR_REPLACE_SOURCE_UNREADABLE)

        Log.debug('Uploading...')
        Server.file_upload(
            local_filename=local_filename,
            remote_filename=self.__filename__,
            allow_overwrite=True
        )
