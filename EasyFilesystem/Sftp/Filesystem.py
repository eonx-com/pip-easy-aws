#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import uuid
from typing import Optional

from EasyFilesystem.BaseFilesystem import BaseFilesystem
from EasyLocalDisk.Client import Client as LocalDiskClient
from EasyLog.Log import Log
from EasyFilesystem.FilesystemError import FilesystemError
from EasyFilesystem.Sftp.Client import Client


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

        self.__client__ = Client()

        # Store server details
        self.__address__ = address
        self.__port__ = port
        self.__base_path__ = Client.sanitize_path(base_path)
        self.__validate_fingerprint__ = validate_fingerprint
        self.__username__ = username
        self.__password__ = password
        self.__rsa_private_key__ = rsa_private_key
        self.__fingerprint__ = fingerprint
        self.__fingerprint_type__ = fingerprint_type

        if validate_fingerprint is False:
            self.__client__.disable_fingerprint_validation()

    def create_path(self, path, allow_overwrite=False) -> None:
        """
        Create path in remote filesystem

        :type path: str
        :param path: The path to create

        :type allow_overwrite: bool
        :param allow_overwrite: Flag indicating the path is allowed to be overwritten if it already exists. If False, and the path exists an exception will be thrown
        """
        path = self.__make_relative_path__(path)

        self.__client__.create_path(path=path, allow_overwrite=allow_overwrite)

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
        path = self.__make_relative_path__(path)

        return self.__client__.file_list(path=path, recursive=recursive)

    def file_exists(self, filename) -> bool:
        """
        Check if file exists

        :type filename: str
        :param filename: Filename/path of the file to check

        :return: bool
        """
        filename = self.__make_relative_path__(filename)

        return self.__client__.file_exists(filename=filename)

    def file_delete(self, filename, allow_missing=False) -> None:
        """
        Delete a file

        :type filename: string
        :param filename: Path of the file to be deleted

        :type allow_missing: bool
        :param allow_missing: Boolean flag, if False and the file cannot be found to delete an exception will be raised
        """
        filename = self.__make_relative_path__(filename)

        self.__client__.file_delete(filename=filename)

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

        self.__client__.file_move(source_filename=source_filename, destination_filename=destination_filename, allow_overwrite=allow_overwrite)

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

        self.__client__.file_copy(source_filename=source_filename, destination_filename=destination_filename, allow_overwrite=allow_overwrite)

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

        return self.__client__.file_download(local_filename=local_filename, remote_filename=remote_filename, allow_overwrite=allow_overwrite)

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

        self.__client__.file_download_recursive(remote_path=remote_path, local_path=local_path, callback=callback, allow_overwrite=allow_overwrite)

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

        return self.__client__.file_upload(local_filename=local_filename, remote_filename=remote_filename, allow_overwrite=allow_overwrite)

    # Download limit tracking

    def reset_file_download_counter(self) -> None:
        """
        Reset the file download counter used to track maximum number of allowed downloads

        :return: None
        """
        Log.trace('Reset Download Counter...')
        self.__count_downloaded__ = 0

    def increment_download_counter(self) -> bool:
        """
        Increase the download counter and return a flag indicating whether the limit has been reached

        :return: bool
        """
        if self.__file_download_limit__ is not None:
            self.__file_download_count__ += 1

        return self.is_file_download_limit_reached()

    def is_file_download_limit_reached(self) -> bool:
        """
        Return boolean flag indicating whether the download limit has been reached

        :return: bool
        """
        if self.__file_download_limit__ is None:
            return False

        return self.__file_download_limit__ - self.__file_download_count__ > 1

    def get_file_download_limit_remaining(self) -> Optional[int]:
        """
        Return the number of file downloads remaining

        :return: int or None
        """
        # If there is download limit set, return None
        if self.__file_download_limit__ is None:
            return None

        # Otherwise return the number of files remaining
        return self.__file_download_limit__ - self.__file_download_count__

    def set_file_download_limit(self, maximum_files) -> None:
        """
        Set a limit on the maximum number of downloads allowed

        :type maximum_files int
        :param maximum_files: The maximum number of files that can be downloaded. Once reached any download requested will be skipped

        :return: None
        """
        Log.trace('Setting Download Limit...')
        self.__file_download_limit__ = maximum_files

    # SFTP specific functions

    def get_address(self) -> str:
        """
        Get the server sftp_address

        :return: str
        """
        return self.__address__

    def get_username(self) -> str:
        """
        Get the sftp_username used to connect

        :return: str
        """
        return self.__username__

    def get_port(self) -> int:
        """
        Get the server sftp_port

        :return: int
        """
        return self.__port__

    def get_base_path(self) -> str:
        """
        Return the current base path

        :return: str
        """
        return self.__base_path__

    # Connection handling functions

    def connect(self) -> bool:
        """
        Connect to SFTP server

        :return: bool
        """
        Log.trace('Connecting to SFTP server...')

        # noinspection PyBroadException
        try:
            if self.__rsa_private_key__ is not None:
                # Connect using RSA private key
                Log.debug('Connecting using RSA private key...')
                self.__sftp_client__.connect_rsa_private_key(
                    address=self.__address__,
                    port=self.__port__,
                    username=self.__username__,
                    rsa_private_key=self.__rsa_private_key__,
                    fingerprint=self.__fingerprint__,
                    fingerprint_type=self.__fingerprint_type__
                )
            else:
                # Connect using sftp_username/password
                Log.debug('Connecting using username/password key...')
                self.__sftp_client__.connect_password(
                    address=self.__address__,
                    port=self.__port__,
                    username=self.__username__,
                    password=self.__password__,
                    fingerprint=self.__fingerprint__,
                    fingerprint_type=self.__fingerprint_type__
                )
        except Exception as connect_exception:
            Log.error('Connection Failed: {connect_exception}'.format(connect_exception=connect_exception))
            return False

        return True

    def disconnect(self) -> None:
        """
        Disconnect from SFTP server

        :return: None
        """
        Log.trace('Disconnecting from SFTP server...')

        self.__sftp_client__.disconnect()

    def is_connected(self) -> bool:
        """
        Check if we are still connected to the SFTP server

        :return: bool
        """
        return self.__sftp_client__.is_connected()

    # Baseline file handling functions

    # Iteration functions

    def __stake_ignore__(self, local_filename, remote_filename):
        """
        Handle staked files as they are downloaded

        :type local_filename: str
        :param local_filename:

        :type remote_filename: str
        :param remote_filename:

        :return:
        """
        # Trigger callback without performing any kind of unique checks
        self.__callback__(local_filename=local_filename, remote_filename=remote_filename, staked_filename=remote_filename)

    def __stake_rename__(self, local_filename, remote_filename):
        """
        Handle staked files as they are downloaded, ensuring we are the only process actioning the file by performing a rename operation

        :type local_filename: str
        :param local_filename:

        :type remote_filename: str
        :param remote_filename:

        :return:
        """
        try:
            # Skip files that are already marked as staked
            if remote_filename.endswith('.staked'):
                os.unlink(local_filename)
                return

            # Rename the file on the remote server so it doesn't get picked up by another staking operation
            staked_filename = '{remote_filename}.{uuid}.staked'.format(remote_filename=remote_filename, uuid=self.__uuid__)
            self.__sftp_client__.file_move(source_filename=remote_filename, destination_filename=staked_filename)

            # Make sure we managed to rename the file before anybody else
            if self.__sftp_client__.file_exists(filename=staked_filename) is False:
                # Somebody else got it first
                os.unlink(local_filename)
                return

            # Trigger callback function
            self.__callback__(local_filename=local_filename, remote_filename=remote_filename, staked_filename=staked_filename)
        except Exception as staking_exception:
            # If anything goes wrong during staking, just ignore it and move on
            Log.warning('Staking Failed, Skipping File: {staking_exception}'.format(staking_exception=staking_exception))
            os.unlink(local_filename)

    # Internal helper methods

    def __sanitize_filename__(self, filename) -> str:
        """
        Returned sanitized path that is relative to the base directory

        :type filename: str
        :param filename: The filename to sanitize

        :return: str
        """
        filename = Client.sanitize_filename(filename)
        return Client.sanitize_filename('{base_path}{filename}'.format(base_path=self.__base_path__, filename=filename))

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
