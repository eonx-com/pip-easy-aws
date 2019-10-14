#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import uuid

from EasyLocalDisk.Client import Client as LocalDiskClient
from EasyLog.Log import Log
from EasySftp.Client import Client
from EasySftp.ServerError import ServerError


class Server:
    ITERATE_STRATEGY_RENAME = 'RENAME'
    ITERATE_STRATEGY_IGNORE = 'IGNORE'

    def __init__(
            self,
            address,
            username,
            password=None,
            rsa_private_key=None,
            fingerprint=None,
            fingerprint_type=None,
            validate_fingerprint=True,
            port=22,
            base_path=''
    ):
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
        Log.trace('Instantiating SFTP server class...')

        self.__uuid__ = uuid.uuid4()
        self.__callback__ = None

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

        # Get an EasySftp client
        self.__sftp_client__ = Client()

        # Disable fingerprint validation if requested
        if validate_fingerprint is False:
            self.__sftp_client__.disable_fingerprint_validation()

        # Raise warning if host key checking is disabled
        if self.__validate_fingerprint__ is False:
            Log.warning('Fingerprint validation has been disabled, this may be a security risk...')

    # Functions to retrieve non-sensitive information

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

    # File handling functions

    def file_list(self, remote_path, recursive=False) -> list:
        """
        List a list of all files accessible in the filesystem filesystem

        :type remote_path: str
        :param remote_path: The path in the filesystem to list

        :type recursive: bool
        :param recursive: If True the listing will proceed recursively down through all sub-folders

        :return: list
        """
        Log.trace('Retrieving file list from SFTP server...')

        files = self.__sftp_client__.file_list(
            remote_path=self.__get_relative_base_path__(remote_path),
            recursive=recursive
        )

        returned_files = []

        for file in files:
            returned_files.append(file[len(self.__base_path__) - 1:])

        return returned_files

    def file_exists(self, remote_filename) -> bool:
        """
        Check if file exists

        :type remote_filename: str
        :param remote_filename: Filename/path of the file to check

        :return: bool
        """
        Log.trace('Checking file exists on SFTP server...')

        return self.__sftp_client__.file_exists(
            remote_filename=self.__get_relative_base_path__(remote_filename)
        )

    def file_delete(self, remote_filename) -> None:
        """
        Delete a file from SFTP server

        :type remote_filename: str
        :param remote_filename: Filename/path of the file to download from the SFTP server

        :return: None
        """
        Log.trace('Deleting file from SFTP server...')

        self.__sftp_client__.file_delete(self.__get_relative_base_path__(remote_filename))

    def file_download(self, local_filename, remote_filename, allow_overwrite=True) -> None:
        """
        Download a file from SFTP server

        :type local_filename: str
        :param local_filename: Filename/path of the destination on the local filesystem

        :type remote_filename: str
        :param remote_filename: Filename/path of the file to download from the SFTP server

        :type allow_overwrite: bool
        :param allow_overwrite: Flag indicating the file is allowed to be overwritten if it already exists. If False, and the file exists an base_exception will be thrown

        :return: None
        """
        Log.trace('Downloading file from SFTP server...')

        self.__sftp_client__.file_download(
            local_filename=local_filename,
            remote_filename=self.__get_relative_base_path__(remote_filename),
            allow_overwrite=allow_overwrite
        )

    def file_download_recursive(self, remote_path, local_path, callback=None, allow_overwrite=True) -> None:
        """
        Recursively download all files found in the specified remote path to the specified local path

        :type remote_path: str
        :param remote_path: Path on SFTP server to be downloaded

        :type local_path: str
        :param local_path: Path on local file system where contents are to be downloaded

        :type callback: function/None
        :param callback: Optional callback function to call after each file has downloaded, must accept a single
            string parameter which contains the local filesystem filename

        :type allow_overwrite: bool
        :param allow_overwrite: Flag indicating the file is allowed to be overwritten if it already exists. If False, and the file exists an base_exception will be thrown

        :return: None
        """
        Log.trace('Starting recursive download from SFTP server...')

        self.__sftp_client__.file_download_recursive(
            remote_path=self.__get_relative_base_path__(remote_path),
            local_path=local_path,
            callback=callback,
            allow_overwrite=allow_overwrite
        )

    def file_upload(self, local_filename, remote_filename, callback=None, confirm=False, allow_overwrite=True) -> None:
        """
        Upload a file to SFTP server

        :type local_filename: str
        :param local_filename: Filename/path of file to be uploaded from local filesystem

        :type remote_filename: str
        :param remote_filename: Filename/path of the destination on the SFTP server

        :type callback: function/None
        :param callback: Optional callback function to report on progress of upload

        :type confirm: bool
        :param confirm: If True, will perform a 'stat' on the uploaded file after the transfer to completes to confirm
            the upload was successful. Note: in situations such as Direct Link where files are removed immediately after
            they have been uploaded, setting this to True will generate an error.

        :type allow_overwrite: bool
        :param allow_overwrite: Flag indicating the file is allowed to be overwritten if it already exists. If False, and the file exists an base_exception will be thrown

        :return: None
        """
        Log.trace('Uploading file to SFTP server...')

        self.__sftp_client__.file_upload(
            local_filename=local_filename,
            remote_filename=self.__get_relative_base_path__(remote_filename),
            callback=callback,
            confirm=confirm,
            allow_overwrite=allow_overwrite
        )

    def file_upload_from_string(self, contents, remote_filename, callback=None, confirm=False, allow_overwrite=True) -> None:
        """
        Upload the contents of a string variable to the SFTP server

        :type contents: str
        :param contents: File contents to be uploaded

        :type remote_filename: str
        :param remote_filename: Filename/path of the destination on the SFTP server

        :type callback: function/None
        :param callback: Optional callback function to report on progress of upload

        :type confirm: bool
        :param confirm: If True, will perform a 'stat' on the uploaded file after the transfer to completes to confirm
            the upload was successful. Note: in situations such as Direct Link where files are removed immediately after
            they have been uploaded, setting this to True will generate an error.

        :type allow_overwrite: bool
        :param allow_overwrite: Flag indicating the file is allowed to be overwritten if it already exists. If False, and the file exists an base_exception will be thrown

        :return: None
        """
        Log.trace('Uploading file (from string) to SFTP server...')

        self.__sftp_client__.file_upload_from_string(
            contents=contents,
            remote_filename=self.__get_relative_base_path__(remote_filename),
            callback=callback,
            confirm=confirm,
            allow_overwrite=allow_overwrite
        )

    # Iteration functions

    def iterate_files(self, callback, strategy, maximum_files=None) -> None:
        """
        :type maximum_files: int or None
        :param maximum_files: Maximum number files to stake

        :type callback: Callable
        :param callback: User callback function executed on every successfully staked file

        :type strategy: str
        :param strategy: Staking strategy to use

        :return: None
        """
        staking_path = LocalDiskClient.create_temp_folder()

        if callable(callback) is False:
            Log.exception(ServerError.ERROR_ITERATE_CALLBACK_NOT_CALLABLE)

        self.__sftp_client__.set_file_download_limit(maximum_files=maximum_files)

        if strategy == Server.ITERATE_STRATEGY_IGNORE:
            callback = self.__stake_ignore__
        elif strategy == Server.ITERATE_STRATEGY_RENAME:
            callback = self.__stake_rename__
        else:
            Log.exception(ServerError.ERROR_ITERATE_STRATEGY_UNKNOWN)

        self.__sftp_client__.file_download_recursive(remote_path='/', local_path=staking_path, callback=callback)

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
            if self.__sftp_client__.file_exists(remote_filename=staked_filename) is False:
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

    def __get_relative_base_path__(self, path) -> str:
        """
        Construct path using the specified base path for the server

        :type path: str
        :param path: The path to mangle

        :return: str
        """
        return Client.sanitize_path('{base_path}/{path}'.format(
            base_path=self.__base_path__,
            path=path
        ))
