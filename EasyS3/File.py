#!/usr/bin/env python
# -*- coding: utf-8 -*-

from EasyS3.Client import Client
from EasyLocalDisk.Client import Client as LocalDiskClient
from EasyLog.Log import Log


# noinspection DuplicatedCode
class File:
    ERROR_REPLACE_SAME_FILE = ''
    ERROR_REPLACE_SOURCE_NOT_FOUND = ''
    ERROR_REPLACE_SOURCE_UNREADABLE = ''

    def __init__(self, bucket_name, bucket_filename):
        """
        :type bucket_name: str
        :param bucket_name:

        :type bucket_filename: str
        :param bucket_filename:
        """
        self.__bucket_name__ = bucket_name
        self.__bucket_filename__ = bucket_filename

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
        if LocalDiskClient.file_readable(filename=local_filename) is False:
            Log.exception(File.ERROR_REPLACE_SOURCE_UNREADABLE)

        Log.debug('Uploading...')
        Client.file_upload(
            local_filename=local_filename,
            bucket_name=self.__bucket_name__,
            bucket_filename=self.__bucket_filename__,
            allow_overwrite=True
        )