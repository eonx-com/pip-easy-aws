#!/usr/bin/env python
# -*- coding: utf-8 -*-

from EasyFilesystem.BaseFilesystem import BaseFilesystem
from EasyFilesystem.FilesystemError import FilesystemError
from EasyFilesystem.S3.Client import Client
from EasyLocalDisk.Client import Client as LocalDiskClient
from EasyLog.Log import Log


class Filesystem(BaseFilesystem):
    def __init__(self, bucket_name, base_path=''):
        """
        Instantiate S3 filesystem

        :type bucket_name: str
        :param bucket_name: The name of the S3 bucket

        :type base_path: str
        :param base_path: Base path inside the the bucket to serve as the filesystem root
        """
        super().__init__()

        self.__client__ = Client()

        # Store bucket details
        self.__base_path__ = Client.sanitize_path(base_path)
        self.__bucket__ = bucket_name

    def create_path(self, path, allow_overwrite=False) -> None:
        """
        Create path in remote filesystem

        :type path: str
        :param path: The path to create

        :type allow_overwrite: bool
        :param allow_overwrite: Flag indicating the path is allowed to be overwritten if it already exists. If False, and the path exists an exception will be thrown
        """
        path = self.__make_relative_path__(path)

        self.__client__.create_path(bucket=self.__bucket__, path=path, allow_overwrite=allow_overwrite)

    def create_temp_path(self, prefix='', temp_path=None) -> str:
        """
        Create a new temporary path that is guaranteed to be unique

        :type prefix: str
        :param prefix: Optional prefix to prepend to path

        :type temp_path: str or None
        :param temp_path: The base path for all temporary files. If None a sensible default should be set

        :return: The path that was created
        """
        if temp_path is None:
            temp_path = '{base_path}/tmp/'.format(base_path=self.__base_path__)
        else:
            temp_path = self.__make_relative_path__(temp_path)

        return self.__client__.create_temp_path(bucket=self.__bucket__, prefix=prefix, temp_path=temp_path)

    def file_list(self, path, recursive=False) -> list:
        """
        List a list of all files accessible in the filesystem filesystem

        :type path: str
        :param path: The path in the filesystem to list

        :type recursive: bool
        :param recursive: If True the listing will proceed recursively down through all sub-folders

        :return: list
        """
        path = self.__make_relative_path__(path)

        return self.__client__.file_list(bucket=self.__bucket__, path=path, recursive=recursive)

    def file_exists(self, filename) -> bool:
        """
        Check if file exists

        :type filename: str
        :param filename: Filename/path of the file to check

        :return: bool
        """
        filename = self.__make_relative_path__(filename)

        return self.__client__.file_exists(bucket=self.__bucket__, filename=filename)

    def file_delete(self, filename, allow_missing=False) -> None:
        """
        Delete a file

        :type filename: string
        :param filename: Path of the file to be deleted

        :type allow_missing: bool
        :param allow_missing: Boolean flag, if False and the file cannot be found to delete an exception will be raised
        """
        filename = self.__make_relative_path__(filename)

        self.__client__.file_delete(bucket=self.__bucket__, filename=filename)

    def file_move(self, source_filename, destination_filename, allow_overwrite=True) -> None:
        """
        Move a file to the specified destination

        :type source_filename: string
        :param source_filename: The source filename

        :type destination_filename: string
        :param destination_filename: The destination filename

        :type allow_overwrite: bool
        :param allow_overwrite: Flag indicating the file is allowed to be overwritten if it already exists. If False, and the file exists an exception will be thrown
        """
        source_filename = self.__make_relative_path__(source_filename)
        destination_filename = self.__make_relative_path__(destination_filename)

        self.__client__.file_move(source_bucket=self.__bucket__, source_filename=source_filename, destination_bucket=self.__bucket__, destination_filename=destination_filename, allow_overwrite=allow_overwrite)

    def file_copy(self, source_filename, destination_filename, allow_overwrite=True) -> None:
        """
        Copy file to the specified destination

        :type source_filename: string
        :param source_filename: The source path/filename

        :type destination_filename: string
        :param destination_filename: The destination path.filename

        :type allow_overwrite: bool
        :param allow_overwrite: Flag indicating the file is allowed to be overwritten if it already exists. If False, and the file exists an exception will be thrown
        """
        source_filename = self.__make_relative_path__(source_filename)
        destination_filename = self.__make_relative_path__(destination_filename)

        self.__client__.file_copy(source_bucket=self.__bucket__, source_filename=source_filename, destination_bucket=self.__bucket__, destination_filename=destination_filename, allow_overwrite=allow_overwrite)

    def file_download(self, local_filename, remote_filename, allow_overwrite=True) -> None:
        """
        Download a file from SFTP server

        :type local_filename: str
        :param local_filename: Filename/path of the destination on the local filesystem

        :type remote_filename: str
        :param remote_filename: Filename/path of the file to download from the SFTP server

        :type allow_overwrite: bool
        :param allow_overwrite: Flag indicating the file is allowed to be overwritten if it already exists. If False, and the file exists an exception will be thrown

        :return: None
        """
        remote_filename = self.__make_relative_path__(remote_filename)

        return self.__client__.file_download(bucket=self.__bucket__, local_filename=local_filename, remote_filename=remote_filename, allow_overwrite=allow_overwrite)

    def file_download_recursive(self, remote_path, local_path, callback=None, allow_overwrite=True) -> None:
        """
        Recursively download all files found in the specified remote path to the specified local path

        :type remote_path: str
        :param remote_path: Path/filename to be downloaded

        :type local_path: str
        :param local_path: Path on local file system where file is to be downloaded

        :type callback: Callable or None
        :param callback: Optional callback function to call after each file has downloaded

        :type allow_overwrite: bool
        :param allow_overwrite: Flag indicating the file is allowed to be overwritten if it already exists. If False, and the file exists an exception will be thrown

        :return: None
        """
        remote_path = self.__make_relative_path__(remote_path)

        self.__client__.file_download_recursive(bucket=self.__bucket__, remote_path=remote_path, local_path=local_path, callback=callback, allow_overwrite=allow_overwrite)

    def file_upload(self, remote_filename, local_filename, callback=None, allow_overwrite=True) -> None:
        """
        Upload a file to remote filesystem

        :type local_filename: str
        :param local_filename: Filename/path of file to be uploaded from local filesystem

        :type remote_filename: str
        :param remote_filename: Filename/path where the file should be uploaded

        :type callback: function/None
        :param callback: Optional callback function on successful upload

        :type allow_overwrite: bool
        :param allow_overwrite: Flag indicating the file is allowed to be overwritten if it already exists. If False, and the file exists an exception will be raised

        :return: None
        """
        remote_filename = self.__make_relative_path__(remote_filename)

        return self.__client__.file_upload(bucket=self.__bucket__, local_filename=local_filename, remote_filename=remote_filename, allow_overwrite=allow_overwrite)

    # Internal helper methods

    def __make_relative_path__(self, filename) -> str:
        """
        Rebase the supplied filename relative to base path

        :type filename: str
        :param filename: The filename to rebase

        :return: str
        """
        relative_filename = '{base_path}{filename}'.format(base_path=self.__base_path__, filename=filename)
        return Client.sanitize_filename(relative_filename)
