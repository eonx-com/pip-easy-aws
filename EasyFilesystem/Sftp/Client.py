#!/usr/bin/env python
# -*- coding: utf-8 -*-

import base64
import os
import paramiko
import socket
import uuid
import warnings

from EasyLocalDisk.Client import Client as LocalDiskClient
from EasyLog.Log import Log
from EasyFilesystem.Sftp.ClientError import ClientError
from io import StringIO
from pysftp import CnOpts
from pysftp import Connection


# noinspection DuplicatedCode
class Client:
    def __init__(self):
        """
        Setup SFTP client
        """
        self.__sftp_connection__ = None
        self.__fingerprint_validation__ = True

    def __del__(self):
        """
        Close any existing SFTP connection on deletion of this object. This is needed to prevent Lambda function from generating an error on completion

        :return: None
        """
        if self.is_connected() is True:
            # noinspection PyBroadException
            try:
                self.__sftp_connection__.close()
            except Exception:
                # Ignore exceptions at this point
                pass

    @staticmethod
    def sanitize_path(path) -> str:
        """
        Sanitize a path

        :type path: str
        :param path: Path to be sanitized

        :return: str
        """
        # Remove all leading/trailing whitespace
        path = str(path).strip()

        # Remove all duplicated slashes in the path
        while '//' in path:
            path = path.replace('//', '/')

        # Strip all leading/trailing slashes
        path = path.strip('/')

        # Add slash to end of string before returning
        return '{path}/'.format(path=path)

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

    def create_path(self, path, allow_overwrite=False) -> None:
        """
        Create path in remote filesystem

        :type path: str
        :param path: The path to create

        :type allow_overwrite: bool
        :param allow_overwrite: Flag indicating the path is allowed to be overwritten if it already exists. If False, and the path exists an exception will be thrown

        :return: None
        """
        # Sanitize the supplied path
        path = Client.sanitize_path(path)

        # Make sure the path does not already exist if overwrite is disabled
        if allow_overwrite is False:
            if self.file_exists(filename=path) is True:
                Log.exception(ClientError.ERROR_CREATE_PATH_ALREADY_EXISTS)

        # Create the path
        try:
            self.__sftp_connection__.makedirs(remotedir=path)
        except Exception as create_path_exception:
            Log.exception(ClientError.ERROR_CREATE_PATH_UNHANDLED_EXCEPTION, create_path_exception)

        # Make sure the path exists
        if self.file_exists(filename=path) is False:
            Log.exception(ClientError.ERROR_CREATE_PATH_FAILED)

    def create_temp_path(self, prefix='', temp_path=None) -> str:
        """
        Create a new temporary path that is guaranteed to be unique

        :type prefix: str
        :param prefix: Optional prefix to prepend to path

        :type temp_path: str
        :param temp_path: The base path for all temporary files

        :return: str
        """
        # If not temp path is set, set a sensible default
        if temp_path is None:
            temp_path = '/temp/'

        # Sanitize the supplied path
        temp_path = Client.sanitize_path(temp_path)

        # Make sure the temporary folder exists
        if self.file_exists(temp_path) is False:
            Log.exception(ClientError.ERROR_CREATE_TEMP_PATH_FOLDER_NOT_FOUND)

        # Enter a loop until we find a folder name that doesn't already exist
        count_attempts = 0
        while True:
            count_attempts += 1
            path = '{temp_path}{prefix}{uuid}'.format(temp_path=temp_path, prefix=prefix, uuid=uuid.uuid4())
            if self.file_exists(path) is False:
                # noinspection PyBroadException
                try:
                    self.create_path(path=path, allow_overwrite=False)
                    return path
                except Exception:
                    # If creation fails its likely because it exists- so we will just try again next time
                    pass
            if count_attempts > 10:
                # If we fail to create a path more than 10 times, something is wrong
                Log.exception(ClientError.ERROR_CREATE_TEMP_PATH_FAILED)

    def file_list(self, path, recursive=False):
        """
        List a list of all files accessible in the filesystem filesystem

        :type path: str
        :param path: The path in the filesystem to list

        :type recursive: bool
        :param recursive: If True the listing will proceed recursively down through all sub-folders

        :return: list
        """
        # Sanitize the path
        path = Client.sanitize_path(path)

        files = []

        try:
            current_path = self.__sftp_connection__.listdir(path)
            for current_item in current_path:
                current_item = Client.sanitize_filename('{remote_path}/{current_item}'.format(remote_path=path, current_item=current_item))
                if self.__sftp_connection__.isdir(current_item):
                    # Current item is a directory, if we are doing a recursive listing, iterate down through it
                    if recursive is True:
                        files.extend(self.file_list(path=current_item, recursive=recursive))
                else:
                    # Current item is a file, add it to the list
                    files.append(Client.sanitize_filename(current_item))
        except Exception as file_list_exception:
            Log.exception(ClientError.ERROR_FILE_LIST_UNHANDLED_EXCEPTION, file_list_exception)

        return files

    def path_exists(self, path) -> bool:
        """
        Check if path exists

        :type path: str
        :param path: Path to check

        :return: bool
        """
        # Sanitize the filename
        path = Client.sanitize_path(path)

        try:
            return self.__sftp_connection__.exists(remotepath=path)
        except Exception as exists_exception:
            Log.exception(ClientError.ERROR_PATH_EXISTS_UNHANDLED_EXCEPTION, exists_exception)

    def file_exists(self, filename) -> bool:
        """
        Check if file exists

        :type filename: str
        :param filename: Filename/path of the file to check

        :return: bool
        """
        # Sanitize the filename
        filename = Client.sanitize_filename(filename)

        try:
            return self.__sftp_connection__.exists(remotepath=filename)
        except Exception as exists_exception:
            Log.exception(ClientError.ERROR_FILE_EXISTS_UNHANDLED_EXCEPTION, exists_exception)

    def path_delete(self, path, allow_missing=False) -> None:
        """
        Delete a path from S3 bucket

        :type path: str
        :param path: Path to be deleted

        :type allow_missing: bool
        :param allow_missing: Boolean flag, if False and the file cannot be found to delete an exception will be raised

        :return: None
        """
        # Sanitize the path
        path = self.sanitize_path(path)

        # Make sure the file exists before we try to delete it
        if self.path_exists(path=path) is False:
            # If this is fine, get out of here- the file is already gone
            if allow_missing is True:
                return

            Log.exception(ClientError.ERROR_PATH_DELETE_NOT_FOUND)

        # Delete the path
        try:
            self.__sftp_connection__.rmdir(path)
        except Exception as delete_exception:
            Log.exception(ClientError.ERROR_PATH_DELETE_UNHANDLED_EXCEPTION, delete_exception)

        # Make sure the file no longer exists after deleting
        if self.path_exists(path=path) is True:
            Log.exception(ClientError.ERROR_PATH_DELETE_FAILED)

    def file_delete(self, filename, allow_missing=False) -> None:
        """
        Delete a file from SFTP server

        :type filename: str
        :param filename: Filename/path of the file to download from the SFTP server

        :type allow_missing: bool
        :param allow_missing: Boolean flag, if False and the file cannot be found to delete an exception will be raised

        :return: None
        """
        # Sanitize the filename
        filename = Client.sanitize_filename(filename)

        # Make sure the file exists before we try to delete it
        if self.file_exists(filename) is False:
            # If this is fine, get out of here- the file is already gone
            if allow_missing is True:
                return
            Log.exception(ClientError.ERROR_FILE_DELETE_NOT_FOUND)

        # Delete the file
        try:
            self.__sftp_connection__.remove(filename)
        except Exception as delete_exception:
            Log.exception(ClientError.ERROR_FILE_DELETE_UNHANDLED_EXCEPTION, delete_exception)

        # Make sure the file no longer exists after deleting
        if self.file_exists(filename) is True:
            Log.exception(ClientError.ERROR_FILE_DELETE_FAILED)

    def file_move(self, source_filename, destination_filename, allow_overwrite=True) -> None:
        """
        Move a file from one location in the filesystem to another

        :type source_filename: str
        :param source_filename: Path/filename to be moved

        :type destination_filename: str
        :param destination_filename: Destination path/filename within the same filesystem

        :type allow_overwrite: bool
        :param allow_overwrite: Flag indicating the file is allowed to be overwritten if it already exists. If False, and the file exists an exception will be thrown

        :return: None
        """
        # Sanitize the filenames
        source_filename = Client.sanitize_filename(source_filename)
        destination_filename = Client.sanitize_filename(destination_filename)

        # Ensure the source and destination are not the same
        if source_filename == destination_filename:
            Log.exception(ClientError.ERROR_FILE_MOVE_SOURCE_DESTINATION_SAME)

        # If allow overwrite is disabled, make sure the file doesn't already exist
        if allow_overwrite is False:
            if self.file_exists(destination_filename) is True:
                Log.exception(ClientError.ERROR_FILE_MOVE_ALREADY_EXISTS)

        # Make sure the source file exists
        if self.file_exists(source_filename) is False:
            Log.exception(ClientError.ERROR_FILE_MOVE_SOURCE_NOT_FOUND)

        # Move the file
        try:
            self.__sftp_connection__.rename(remote_src=source_filename, remote_dest=destination_filename)
        except Exception as move_exception:
            Log.exception(ClientError.ERROR_FILE_MOVE_UNHANDLED_EXCEPTION, move_exception)

        # Make sure the file exists at its destination
        if self.file_exists(destination_filename) is False:
            Log.exception(ClientError.ERROR_FILE_MOVE_COPY_FAILED)

        # Make sure the source file no longer exists
        if self.file_exists(source_filename) is True:
            Log.exception(ClientError.ERROR_FILE_MOVE_DELETE_FAILED)

    def file_copy(self, source_filename, destination_filename, allow_overwrite=True) -> None:
        """
        Copy a file from one location in the filesystem to another

        :type source_filename: str
        :param source_filename: Path/filename to be moved

        :type destination_filename: str
        :param destination_filename: Destination path/filename within the same filesystem


        :type allow_overwrite: bool
        :param allow_overwrite: Flag indicating the file is allowed to be overwritten if it already exists. If False, and the file exists an exception will be thrown

        :return: None
        """
        # Sanitize the filenames
        source_filename = Client.sanitize_filename(source_filename)
        destination_filename = Client.sanitize_filename(destination_filename)

        # Ensure the source and destination are not the same
        if source_filename == destination_filename:
            Log.exception(ClientError.ERROR_FILE_COPY_SOURCE_DESTINATION_SAME)

        # If we are not in overwrite mode, we need to check if the file exists already
        if allow_overwrite is False:
            if self.file_exists(destination_filename) is True:
                Log.exception(ClientError.ERROR_FILE_COPY_ALREADY_EXISTS)

        # Make sure the source file exists
        if self.file_exists(source_filename) is False:
            Log.exception(ClientError.ERROR_FILE_COPY_SOURCE_NOT_FOUND)

        # To copy a file we need to download/upload it, storing it in a temporary file. Create a unique location to store it.
        temp_folder = LocalDiskClient.create_temp_path()
        temp_filename = '{temp_folder}/{uuid}'.format(temp_folder=temp_folder, uuid=uuid.uuid4())

        try:
            # Download/upload the file
            self.file_download(local_filename=temp_filename, remote_filename=source_filename)
            self.file_upload(local_filename=temp_filename, remote_filename=destination_filename)
            # Delete the temporary file
            LocalDiskClient.file_delete(temp_folder)
        except Exception as copy_exception:
            # Something went wrong- attempt to cleanup locally
            if LocalDiskClient.file_exists(temp_folder) is True:
                # noinspection PyBroadException
                try:
                    LocalDiskClient.file_delete(temp_folder)
                except Exception:
                    Log.warning('Failed to cleanup temporary file during copy operation')
                    pass
            Log.exception(ClientError.ERROR_FILE_COPY_UNHANDLED_EXCEPTION, copy_exception)

        # Make sure the file exists at the destination
        if self.file_exists(source_filename) is False:
            Log.exception(ClientError.ERROR_FILE_COPY_FAILED)

    def file_download(self, local_filename, remote_filename, allow_overwrite=True) -> None:
        """
        Download a file

        :type local_filename: str
        :param local_filename: Filename/path to destination on local filesystem where the file will be downloaded to

        :type remote_filename: str
        :param remote_filename: Filename/path of the remote file to be downloaded

        :type allow_overwrite: bool
        :param allow_overwrite: Flag indicating the file is allowed to be overwritten if it already exists. If False, and the file exists an exception will be raised

        :return: None
        """
        # Sanitize the filenames
        remote_filename = Client.sanitize_filename(remote_filename)

        # If allow overwrite is disabled, make sure the file doesn't already exist
        if allow_overwrite is False:
            if LocalDiskClient.file_exists(local_filename) is True:
                Log.exception(ClientError.ERROR_FILE_DOWNLOAD_ALREADY_EXISTS)

        # Make sure the file exists at the source
        if self.file_exists(remote_filename) is False:
            Log.exception(ClientError.ERROR_FILE_DOWNLOAD_SOURCE_NOT_FOUND)

        # Download the file
        try:
            # Make sure the local download path exists
            destination_path = LocalDiskClient.sanitize_path(os.path.dirname(local_filename))
            LocalDiskClient.create_path(destination_path, allow_overwrite=True)
            self.__sftp_connection__.get(remotepath=remote_filename, localpath=local_filename, preserve_mtime=True)
        except Exception as download_exception:
            Log.exception(ClientError.ERROR_FILE_DOWNLOAD_UNHANDLED_EXCEPTION, download_exception)

        # Make sure the file now exists locally
        if os.path.exists(local_filename) is False:
            Log.exception(ClientError.ERROR_FILE_DOWNLOAD_FAILED)

    def file_download_recursive(self, remote_path, local_path, callback=None, allow_overwrite=True) -> None:
        """
        Recursively download all files found in the specified remote path to the specified local path

        :type remote_path: str
        :param remote_path: Path on SFTP server to be downloaded

        :type local_path: str
        :param local_path: Path on local file system where contents are to be downloaded

        :type callback: function/None
        :param callback: Optional callback function to call after each file has downloaded successfully

        :type allow_overwrite: bool
        :param allow_overwrite: Flag indicating the file is allowed to be overwritten if it already exists. If False, and the file exists an exception will be thrown

        :return: None
        """
        # Sanitize the paths
        local_path = LocalDiskClient.sanitize_path(local_path)
        remote_path = Client.sanitize_path(remote_path)

        # List files in current path
        files_found = self.file_list(path=remote_path, recursive=True)

        # Iterate these files
        for current_remote_filename in files_found:
            # The local filename will stored in the same folder structure as on the SFTP server
            current_local_path = LocalDiskClient.sanitize_path(local_path + os.path.dirname(current_remote_filename))
            current_local_filename = LocalDiskClient.sanitize_filename(current_local_path + os.path.basename(current_remote_filename))

            # Make sure the current files path exists locally before we start downloading
            LocalDiskClient.create_path(path=current_local_path, allow_overwrite=True)

            # Download the current file
            self.file_download(local_filename=current_local_filename, remote_filename=current_remote_filename, allow_overwrite=allow_overwrite)

            # If a callback was supplied execute it
            if callback is not None:
                if callable(callback) is False:
                    Log.exception(ClientError.ERROR_FILE_DOWNLOAD_CALLBACK_NOT_CALLABLE)
                # If the callback returns false, stop iterating
                if callback(local_filename=current_local_filename, remote_filename=current_remote_filename) is False:
                    break

    def file_upload(self, local_filename, remote_filename, allow_overwrite=True) -> None:
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
        # Sanitize the filenames
        local_filename = LocalDiskClient.sanitize_filename(local_filename)
        remote_filename = Client.sanitize_filename(remote_filename)

        # Make sure the local file exists
        if LocalDiskClient.file_exists(local_filename) is False:
            Log.exception(ClientError.ERROR_FILE_UPLOAD_SOURCE_NOT_FOUND)

        # Make sure the file doesn't already exist if overwrite is disabled
        if allow_overwrite is False:
            if self.file_exists(remote_filename) is True:
                raise Exception(ClientError.ERROR_FILE_UPLOAD_ALREADY_EXISTS)

        # Upload the file
        try:
            # Make sure the destination path exists on the SFTP server
            self.create_path(os.path.dirname(remote_filename), allow_overwrite=True)
            self.__sftp_connection__.put(localpath=local_filename, remotepath=remote_filename, confirm=False)
        except Exception as upload_exception:
            Log.exception(ClientError.ERROR_FILE_UPLOAD_UNHANDLED_EXCEPTION, upload_exception)

        # Make sure the uploaded file exists
        if self.file_exists(remote_filename) is False:
            raise Exception(ClientError.ERROR_FILE_UPLOAD_FAILED)

    def enable_fingerprint_validation(self) -> None:
        """
        Enable host fingerprint checking on connection

        :return: None
        """
        Log.trace('Enable Fingerprint Validation...')
        self.__fingerprint_validation__ = True

    def disable_fingerprint_validation(self) -> None:
        """
        Disable host fingerprint checking on connection

        :return: None
        """
        Log.trace('Disable Fingerprint Validation...')
        self.__fingerprint_validation__ = False

    def connect(self, username, address, port, rsa_private_key=None, password=None, fingerprint=None, fingerprint_type=None) -> None:
        """
        Connect by inspecting which authentication details were supplied and selecting a best guess method

        :type username: str
        :param username: Username to be used in connection

        :type address: str
        :param address: SFTP server sftp_address/hostname

        :type port: int
        :param port: SFTP server sftp_port number (defaults to 22)

        :type rsa_private_key: str
        :param rsa_private_key: RSA private key to be used for authentication

        :type password: str or None
        :param password: Optional password used for authentication

        :type fingerprint: str or None
        :param fingerprint: SFTP server sftp_fingerprint used to validate server identity

        :type fingerprint_type: str or None
        :param fingerprint_type: SFTP server sftp_fingerprint type
        """
        if rsa_private_key is not None:
            # Private key was set, use it and connecting using RSA authentication
            self.connect_rsa_private_key(address=address, port=port, username=username, rsa_private_key=rsa_private_key, password=password, fingerprint=fingerprint, fingerprint_type=fingerprint_type)
        else:
            # Private key was not set, use a username/password
            self.connect_password(address=address, port=port, username=username, password=password, fingerprint=fingerprint, fingerprint_type=fingerprint_type)

    def connect_rsa_private_key(self, username, address, port, rsa_private_key, password=None, fingerprint=None, fingerprint_type=None) -> None:
        """
        Connect to SFTP server using private key authentication

        :type username: str
        :param username: Username to be used in connection

        :type address: str
        :param address: SFTP server sftp_address/hostname

        :type port: int
        :param port: SFTP server sftp_port number (defaults to 22)

        :type rsa_private_key: str
        :param rsa_private_key: RSA private key to be used for authentication

        :type password: str or None
        :param password: Optional password used for authentication

        :type fingerprint: str or None
        :param fingerprint: SFTP server sftp_fingerprint used to validate server identity

        :type fingerprint_type: str or None
        :param fingerprint_type: SFTP server sftp_fingerprint type

        :return: None
        """
        Log.trace('Connecting (RSA Private Key)...')

        # We need to ignore the warning due to non-existence of known_hosts file in Lambda
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")

            Log.debug('SFTP Server: {username}@{address}:{port}'.format(address=address, port=port, username=username))
            Log.debug('RSA Key Length: {key_length} Bytes'.format(key_length=len(rsa_private_key)))

            if fingerprint is not None:
                Log.debug('Host Fingerprint: {fingerprint}'.format(fingerprint=fingerprint))
                Log.debug('Host Fingerprint Type: {fingerprint_type}'.format(fingerprint_type=fingerprint_type))

            try:
                address = Client.__sanitize_sftp_address__(address)
                port = Client.__sanitize_sftp_port__(port)
                rsa_private_key = Client.__sanitize_sftp_private_key__(rsa_private_key)
                username = Client.__sanitize_sftp_username__(username)
                fingerprint = Client.__sanitize_sftp_fingerprint__(fingerprint)
                fingerprint_type = Client.__sanitize_sftp_fingerprint_type__(fingerprint_type)

                connection_options = self.__get_connection_options__(
                    address=address,
                    fingerprint=fingerprint,
                    fingerprint_type=fingerprint_type
                )

                self.__sftp_connection__ = Connection(
                    address,
                    port=port,
                    private_key=rsa_private_key,
                    username=username,
                    password=password,
                    cnopts=connection_options
                )
            except Exception as connection_exception:
                if 'no hostkey for host' in str(connection_exception).lower():
                    raise Exception(ClientError.ERROR_CONNECT_INVALID_FINGERPRINT, connection_exception)

                Log.exception(ClientError.ERROR_CONNECT_FAILED, connection_exception)

            # Assert the connection was successful and didn't generate unexpected warnings
            Client.__assert_connection_warning__(w)

            # Turn warnings back on
            warnings.simplefilter("default")

    def connect_password(self, address, port, username, password, fingerprint=None, fingerprint_type=None) -> None:
        """
        Connect to SFTP server using sftp_username and password

        :type username: str
        :param username: Username to be used in connection

        :type address: str
        :param address: SFTP server sftp_address/hostname

        :type port: int
        :param port: SFTP server sftp_port number

        :type password: str
        :param password: Password for authentication

        :type fingerprint: str or None
        :param fingerprint: SFTP server sftp_fingerprint used to validate server identity

        :type fingerprint_type: str or None
        :param fingerprint_type: SFTP server sftp_fingerprint type

        :return: None
        """
        Log.trace('Connecting (Username/Password)...')

        # We need to ignore the warning due to non-existence of known_hosts file in Lambda
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")

            Log.debug('SFTP Server: {username}@{address}:{port}'.format(address=address, port=port, username=username))
            Log.debug('Host Fingerprint: {fingerprint} ({fingerprint_type})'.format(fingerprint=fingerprint, fingerprint_type=fingerprint_type))

            address = Client.__sanitize_sftp_address__(address)
            port = Client.__sanitize_sftp_port__(port)
            username = Client.__sanitize_sftp_username__(username)
            fingerprint = Client.__sanitize_sftp_fingerprint__(fingerprint)
            fingerprint_type = Client.__sanitize_sftp_fingerprint_type__(fingerprint_type)

            connection_options = self.__get_connection_options__(
                address=address,
                fingerprint=fingerprint,
                fingerprint_type=fingerprint_type
            )

            try:
                self.__sftp_connection__ = Connection(
                    address,
                    port=port,
                    username=username,
                    password=password,
                    cnopts=connection_options
                )
            except Exception as connection_exception:
                if 'no hostkey for host' in str(connection_exception).lower():
                    Log.exception(ClientError.ERROR_CONNECT_INVALID_FINGERPRINT, connection_exception)

                Log.exception(ClientError.ERROR_CONNECT_FAILED, connection_exception)

            # Assert the connection was successful and didn't generate unexpected warnings
            Client.__assert_connection_warning__(w)

            # Turn warnings back on
            warnings.simplefilter("default")

    def disconnect(self):
        """
        Disconnect from SFTP server

        :return: None
        """
        Log.trace('Disconnecting...')
        self.__sftp_connection__.close()

    def is_connected(self):
        """
        Check if we are still connected to the SFTP server

        :return: bool
        """
        # noinspection PyBroadException
        try:
            # If no connection has ever been established return false
            if self.__sftp_connection__ is None:
                return False

            # If there is no SFTP client inside the connection object, return false
            if self.__sftp_connection__.sftp_client is None:
                return False

            # If there is no SFTP channel inside the client object, return false
            if self.__sftp_connection__.sftp_client.get_channel() is None:
                return False

            # If there is no SFTP transport, return false
            if self.__sftp_connection__.sftp_client.get_channel().get_transport() is None:
                return False

            # Otherwise return the current state of the underlying transport layer
            if self.__sftp_connection__.sftp_client.get_channel().get_transport().is_active() is True:
                return True

            return False

        except Exception:
            return False

    # Internal methods

    @staticmethod
    def __assert_connection_warning__(warning) -> None:
        """
        Assert the expected connection warning was received, throwing an exception if not

        :type warning: dict
        :param warning: The warning received

        :return: None
        """
        # If we didn't receive a warning then all is fine
        if -1 not in warning:
            return

        # Otherwise check the category of the warning, it should only be a UserWarning
        warning_category = warning[-1].category

        if issubclass(warning_category, UserWarning) is True:
            warning_message = str(warning[-1].message)
            # Check the message is as expected
            if 'Failed to load HostKeys' not in warning_message or 'You will need to explicitly load HostKeys' not in warning_message:
                raise Exception(ClientError.ERROR_CONNECT_FAILED)

            return

        # Unexpected warning received
        raise Exception(ClientError.ERROR_CONNECT_FAILED)

    @staticmethod
    def __fingerprint_to_paramiko_key__(fingerprint, fingerprint_type):
        """
        Convert and SFTP private key to key suitable for use by underlying paramiko library

        :type fingerprint: str
        :param fingerprint: SFTP server sftp_fingerprint

        :type fingerprint_type: str
        :param fingerprint_type: SFTP server sftp_fingerprint type (e.g. ssh-rsa, ssh-dss)

        :return: paramiko.RSAKey or paramiko.DSSKey or paramiko/ECDSAKey
        """
        try:
            if fingerprint_type == 'ssh-rsa':
                return paramiko.RSAKey(data=base64.b64decode(fingerprint))
            elif fingerprint_type == 'ssh-dss':
                return paramiko.DSSKey(data=base64.b64decode(fingerprint))
            elif fingerprint_type in paramiko.ecdsakey.ECDSAKey.supported_key_format_identifiers():
                return paramiko.ECDSAKey(data=base64.b64decode(fingerprint), validate_point=False)
        except Exception as key_exception:
            Log.exception(ClientError.ERROR_CONNECT_SANITIZE_FINGERPRINT_TYPE, key_exception)

        Log.exception(ClientError.ERROR_CONNECT_SANITIZE_FINGERPRINT_TYPE)

    @staticmethod
    def __sanitize_sftp_port__(port):
        """
        Validate that an acceptable port number was specified

        :type port: int/str/None
        :param port: SFTP server port number (must be in range of 0-65535)

        :return: int
        """
        # If no port was specified generate an error
        if not port:
            Log.exception(ClientError.ERROR_CONNECT_SANITIZE_PORT)

        # If the port number was passed in as a string, make sure it contains digits
        if type(port) is str:
            if not port.isdigit():
                Log.exception(ClientError.ERROR_CONNECT_SANITIZE_PORT)

        # Cast value to an integer
        try:
            port = int(port)
        except ValueError:
            Log.exception(ClientError.ERROR_CONNECT_SANITIZE_PORT)

        # Ensure port number is in valid range
        if port < 0 or port > 65535:
            Log.exception(ClientError.ERROR_CONNECT_SANITIZE_PORT)

        # Return the valid sftp_port number as an integer value
        return port

    @staticmethod
    def __sanitize_sftp_fingerprint_type__(fingerprint_type):
        """
        Validate that the specified SFTP fingerprint type was valid

        :type fingerprint_type: str/None
        :param fingerprint_type: SFTP server fingerprint type (e.g. ssh-rsa)

        :return: str
        """
        # If no fingerprint type was specified return None
        if not fingerprint_type:
            return None

        # Convert fingerprint type to lowercase
        fingerprint_type = str(fingerprint_type).lower()

        # Make sure the key type is one of the acceptable values
        if fingerprint_type not in ['ssh-rsa', 'ssh-dss']:
            if fingerprint_type not in paramiko.ecdsakey.ECDSAKey.supported_key_format_identifiers():
                Log.exception(ClientError.ERROR_CONNECT_SANITIZE_FINGERPRINT_TYPE)

        # Return the valid fingerprint type
        return fingerprint_type

    @staticmethod
    def __sanitize_sftp_username__(sftp_username):
        """
        Validate a username was supplied

        :type sftp_username: str/None
        :param sftp_username: SFTP username

        :return: str
        """
        # If no username was specified generate an error
        if not sftp_username:
            Log.exception(ClientError.ERROR_CONNECT_SANITIZE_USERNAME)

        # Return the username
        return str(sftp_username)

    @staticmethod
    def __sanitize_sftp_private_key__(sftp_private_key):
        """
        Validate the specified SFTP private key was valid and return as a suitable paramiko RSA key value

        :type sftp_private_key: str/None
        :param sftp_private_key: SFTP private key used for connection

        :return: paramiko.RSAKey
        """
        # If no private key was specified generate an error
        if not sftp_private_key:
            Log.exception(ClientError.ERROR_CONNECT_SANITIZE_PRIVATE_KEY)

        # Convert the private key string to a paramiko RSA key that can be used by the underlying libra4y
        try:
            sftp_private_key_string_io = StringIO(str(sftp_private_key))
            return paramiko.RSAKey.from_private_key(sftp_private_key_string_io)
        except Exception as key_exception:
            Log.exception(ClientError.ERROR_CONNECT_SANITIZE_PRIVATE_KEY, key_exception)

    @staticmethod
    def __sanitize_sftp_fingerprint__(fingerprint):
        """
        Validate the specified SFTP fingerprint was valid

        :type fingerprint: str/None
        :param fingerprint: SFTP fingerprint

        :return: str
        """
        # If no fingerprint was specified return None
        if not fingerprint:
            return None

        # Return the fingerprint
        return str(fingerprint)

    @staticmethod
    def __sanitize_sftp_address__(address):
        """
        Validate the specified SFTP server hostname/address is valid and can be resolved successfully

        :type address: str/None
        :param address: SFTP server hostname/address

        :return: str
        """
        # Validate host address was supplied
        if not address:
            Log.exception(ClientError.ERROR_CONNECT_SANITIZE_ADDRESS)

        # Validate host address can be resolved
        try:
            socket.gethostbyname(address)
        except socket.error:
            Log.exception(ClientError.ERROR_CONNECT_SANITIZE_ADDRESS)

        # Return the address
        return str(address)

    def __get_connection_options__(self, address, fingerprint, fingerprint_type):
        """
        Get connection settings for pysftp client

        :type address: str
        :param address: SFTP server sftp_address/hostname

        :type fingerprint: str/None
        :param fingerprint: SFTP server sftp_fingerprint used to validate server identity. If not specified the known_hosts file on the host machine will be used

        :type fingerprint_type: str/None
        :param fingerprint_type: SFTP server sftp_fingerprint type (e.g. ssh-rsa, ssh-dss). This must be one of the key types supported by the underlying paramiko library

        :return: obj
        """
        Log.trace('Retrieving Connection Options...')
        options = CnOpts()

        if self.__fingerprint_validation__ is False:
            Log.warning('Host sftp_fingerprint checking disabled, this may be a security risk...')
            options.hostkeys = None
        else:
            # If a valid sftp_fingerprint and type were specified, add these to the known hosts, otherwise pysftp will use
            # the known_hosts file on the computer
            if fingerprint is not None and fingerprint_type is not None:
                Log.debug('Adding known host sftp_fingerprint to client...')
                options.hostkeys.add(
                    hostname=address,
                    keytype=fingerprint_type,
                    key=self.__fingerprint_to_paramiko_key__(fingerprint, fingerprint_type)
                )
            else:
                Log.warning('No host fingerprints added, relying on known_hosts file')

        return options
