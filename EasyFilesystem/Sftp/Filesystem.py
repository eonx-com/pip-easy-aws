#!/usr/bin/env python
# -*- coding: utf-8 -*-

from EasyFilesystem.BaseFilesystem import BaseFilesystem
from EasyFilesystem.Sftp.Client import Client
from EasyLog.Log import Log


class Filesystem(BaseFilesystem):
    def __init__(self, address, username, password=None, rsa_private_key=None, fingerprint=None, fingerprint_type=None, validate_fingerprint=True, port=22, base_path=''):
        """
        Setup SFTP server

        :type address: str
        :param address: Host server sftp_address/IP sftp_address

        :type port: int
        :param port: SFTP sftp_port number

        :type username: str
        :param username: Username for authentication

        :type rsa_private_key: str
        :param rsa_private_key: Private key for authentication

        :type password: str
        :param password: Password for authentication

        :type fingerprint: str
        :param fingerprint: Host sftp_fingerprint

        :type fingerprint_type: str
        :param fingerprint_type: Host sftp_fingerprint type

        :type validate_fingerprint: bool
        :param validate_fingerprint: Flag indicating SFTP server fingerprint should be validated

        :type base_path: str
        :param base_path: Base SFTP file path, all uploads/downloads will have this path prepended
        """
        super().__init__()

        # Sanitize the supplied base path
        self.__base_path__ = Client.sanitize_path(base_path)

        # Grab SFTP client
        self.__client__ = Client()

        # If requested, disable fingerprint checking
        if validate_fingerprint is False:
            self.__client__.disable_fingerprint_validation()

        # Connect to the server
        self.__client__.connect(
            username=username,
            address=address,
            port=port,
            rsa_private_key=rsa_private_key,
            password=password,
            fingerprint=fingerprint,
            fingerprint_type=fingerprint_type
        )

    def __del__(self):
        """
        Cleanup filesystem, removing any temporary files and shutdown connection
        """
        # Delete all temp folders
        for temp_path in self.__temp_paths__:
            self.__client__.path_delete(
                path=temp_path,
                allow_missing=True
            )

    def create_path(self, path, allow_overwrite=False) -> None:
        """
        Create path in remote filesystem

        :type path: str
        :param path: The path to create

        :type allow_overwrite: bool
        :param allow_overwrite: Flag indicating the path is allowed to be overwritten if it already exists. If False, and the path exists an exception will be thrown
        """
        self.__client__.create_path(path=self.__rebase_path__(path), allow_overwrite=allow_overwrite)

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
            temp_path = self.__rebase_path__(temp_path)

        return self.__client__.create_temp_path(prefix=prefix, temp_path=temp_path)

    def file_list(self, path, recursive=False) -> list:
        """
        List a list of all files accessible in the filesystem filesystem

        :type path: str
        :param path: The path in the filesystem to list

        :type recursive: bool
        :param recursive: If True the listing will proceed recursively down through all sub-folders

        :return: list
        """
        return self.__client__.file_list(path=self.__rebase_path__(path), recursive=recursive)

    def path_exists(self, path) -> bool:
        """
        Check if path exists

        :type path: str
        :param path: Path to check

        :return: bool
        """
        return self.file_exists(path)

    def file_exists(self, filename) -> bool:
        """
        Check if file exists

        :type filename: str
        :param filename: Filename/path of the file to check

        :return: bool
        """
        filename = self.__rebase_path__(filename)
        return self.__client__.file_exists(filename=filename)

    def path_delete(self, path, allow_missing=False) -> None:
        """
        Delete a path

        :type path:str
        :param path: Path to be deleted

        :type allow_missing: bool
        :param allow_missing: Boolean flag, if False and the file cannot be found to delete an exception will be raised
        """
        self.__client__.path_delete(path=self.__rebase_path__(path), allow_missing=allow_missing)

    def file_delete(self, filename, allow_missing=False) -> None:
        """
        Delete a file

        :type filename:str
        :param filename: Path of the file to be deleted

        :type allow_missing: bool
        :param allow_missing: Boolean flag, if False and the file cannot be found to delete an exception will be raised
        """
        self.__client__.file_delete(filename=self.__rebase_path__(filename), allow_missing=allow_missing)

    def file_move(self, source_filename, destination_filename, allow_overwrite=True) -> None:
        """
        Move a file to the specified destination

        :type source_filename:str
        :param source_filename: The source filename

        :type destination_filename:str
        :param destination_filename: The destination filename

        :type allow_overwrite: bool
        :param allow_overwrite: Flag indicating the file is allowed to be overwritten if it already exists. If False, and the file exists an exception will be thrown
        """
        self.__client__.file_move(
            source_filename=self.__rebase_path__(source_filename),
            destination_filename=self.__rebase_path__(destination_filename),
            allow_overwrite=allow_overwrite
        )

    def file_copy(self, source_filename, destination_filename, allow_overwrite=True) -> None:
        """
        Copy file to the specified destination

        :type source_filename:str
        :param source_filename: The source path/filename

        :type destination_filename:str
        :param destination_filename: The destination path.filename

        :type allow_overwrite: bool
        :param allow_overwrite: Flag indicating the file is allowed to be overwritten if it already exists. If False, and the file exists an exception will be thrown
        """
        self.__client__.file_copy(
            source_filename=self.__rebase_path__(source_filename),
            destination_filename=self.__rebase_path__(destination_filename),
            allow_overwrite=allow_overwrite
        )

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
        return self.__client__.file_download(
            local_filename=local_filename,
            remote_filename=self.__rebase_path__(remote_filename),
            allow_overwrite=allow_overwrite
        )

    def file_download_recursive(self, remote_path, local_path, callback=None, allow_overwrite=True) -> None:
        """
        Recursively download all files found in the specified remote path to the specified local path

        :type remote_path: str
        :param remote_path: Path/filename to be downloaded

        :type local_path: str
        :param local_path: Path on local file system where file is to be downloaded

        :type callback: Callable or None
        :param callback: Optional callback_staked function to call after each file has downloaded

        :type allow_overwrite: bool
        :param allow_overwrite: Flag indicating the file is allowed to be overwritten if it already exists. If False, and the file exists an exception will be thrown

        :return: None
        """
        Log.test('Recursive Download Starting...')

        self.__client__.file_download_recursive(
            remote_path=self.__rebase_path__(remote_path),
            local_path=local_path,
            callback=callback,
            allow_overwrite=allow_overwrite
        )

    def file_upload(self, remote_filename, local_filename, allow_overwrite=True) -> None:
        """
        Upload a file to remote filesystem

        :type local_filename: str
        :param local_filename: Filename/path of file to be uploaded from local filesystem

        :type remote_filename: str
        :param remote_filename: Filename/path where the file should be uploaded

        :type allow_overwrite: bool
        :param allow_overwrite: Flag indicating the file is allowed to be overwritten if it already exists. If False, and the file exists an exception will be raised

        :return: None
        """
        return self.__client__.file_upload(
            local_filename=local_filename,
            remote_filename=self.__rebase_path__(remote_filename),
            allow_overwrite=allow_overwrite
        )

    # Internal helper methods

    def __rebase_path__(self, filename) -> str:
        """
        Rebase the supplied filename relative to base path

        :type filename: str
        :param filename: The filename to rebase

        :return: str
        """
        base_path_length = len(self.__base_path__)
        prefix = filename[:base_path_length]

        if prefix == self.__base_path__:
            # Already relative to base path
            return filename

        relative_filename = '{base_path}{filename}'.format(base_path=self.__base_path__, filename=filename)
        return Client.sanitize_filename(relative_filename)
