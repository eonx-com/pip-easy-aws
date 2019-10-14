#!/usr/bin/env python
# -*- coding: utf-8 -*-

import base64
import os
import paramiko
import socket
import warnings

from EasyFilesystem.Helpers import Helpers
from EasyLocalDisk.Client import Client as ClientLocalDisk
from EasyLog.Log import Log
from EasySftp.ClientError import ClientError
from io import StringIO
from pysftp import CnOpts
from pysftp import Connection


# noinspection PyBroadException
class Client:
    def __init__(self):
        """
        Setup SFTP client
        """
        Log.trace('Instantiating SFTP Client...')

        self.__connection__ = None
        self.__count_downloaded__ = 0
        self.__maximum_files__ = None
        self.__fingerprint_validation__ = True

    def __del__(self):
        """
        Close any existing SFTP connection on deletion of this object. This is needed to prevent Lambda function from generating an error on completion
        """
        Log.trace('Destroying SFTP Client...')

        if self.is_connected() is True:
            # noinspection PyBroadException
            try:
                self.__connection__.close()
            except Exception:
                # Ignore exceptions at this point
                pass

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
                    key=self.fingerprint_to_paramiko_key(fingerprint, fingerprint_type)
                )
            else:
                Log.warning('No host fingerprints added, relying on known_hosts file')

        return options

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

    def connect_rsa_private_key(
            self,
            username,
            address,
            port,
            rsa_private_key,
            password=None,
            fingerprint=None,
            fingerprint_type=None
    ) -> None:
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
                address = Client.validate_sftp_address(address)
                port = Client.validate_sftp_port(port)
                rsa_private_key = Client.validate_sftp_private_key(rsa_private_key)
                username = Client.validate_sftp_username(username)
                fingerprint = Client.validate_sftp_fingerprint(fingerprint)
                fingerprint_type = Client.validate_sftp_fingerprint_type(fingerprint_type)

                connection_options = self.__get_connection_options__(
                    address=address,
                    fingerprint=fingerprint,
                    fingerprint_type=fingerprint_type
                )

                self.__connection__ = Connection(
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

    def connect_password(
            self,
            address,
            port,
            username,
            password,
            fingerprint=None,
            fingerprint_type=None
    ) -> None:
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

            address = Client.validate_sftp_address(address)
            port = Client.validate_sftp_port(port)
            username = Client.validate_sftp_username(username)
            fingerprint = Client.validate_sftp_fingerprint(fingerprint)
            fingerprint_type = Client.validate_sftp_fingerprint_type(fingerprint_type)

            connection_options = self.__get_connection_options__(
                address=address,
                fingerprint=fingerprint,
                fingerprint_type=fingerprint_type
            )

            try:
                self.__connection__ = Connection(
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
        self.__connection__.close()

    def is_connected(self):
        """
        Check if we are still connected to the SFTP server

        :return: bool
        """
        # noinspection PyBroadException
        try:
            # If no connection has ever been established return false
            if self.__connection__ is None:
                return False

            # If there is no SFTP client inside the connection object, return false
            if self.__connection__.sftp_client is None:
                return False

            # If there is no SFTP channel inside the client object, return false
            if self.__connection__.sftp_client.get_channel() is None:
                return False

            # If there is no SFTP transport, return false
            if self.__connection__.sftp_client.get_channel().get_transport() is None:
                return False

            # Otherwise return the current state of the underlying transport layer
            if self.__connection__.sftp_client.get_channel().get_transport().is_active() is True:
                return True

            return False

        except Exception:
            return False

    def reset_file_download_counter(self):
        """
        Reset the file download counter used to track maximum number of allowed downloads

        :return:
        """
        Log.trace('Reset Download Counter...')
        self.__count_downloaded__ = 0

    def set_file_download_limit(self, maximum_files) -> None:
        """
        Set a limit on the maximum number of downloads allowed

        :type maximum_files int
        :param maximum_files: The maximum number of files that can be downloaded. Once reached any download requested will be skipped

        :return: None
        """
        Log.trace('Setting Download Limit...')
        self.__maximum_files__ = maximum_files

    def file_list(self, remote_path, recursive=False):
        """
        List a list of all files accessible in the filesystem filesystem

        :type remote_path: str
        :param remote_path: The path in the filesystem to list

        :type recursive: bool
        :param recursive: If True the listing will proceed recursively down through all sub-folders

        :return: list
        """
        Log.trace('Listing Files: {remote_path}...'.format(remote_path=remote_path))

        files = []
        try:
            current_path = self.__connection__.listdir(remote_path)
            for current_filename in current_path:
                current_filename = Client.sanitize_path(remote_path + '/' + current_filename)
                if self.__connection__.isdir(current_filename) and recursive is True:
                    # Iterate down to next folder
                    Log.debug('Directory: {current_filename}'.format(current_filename=current_filename))
                    files.extend(self.file_list(remote_path=current_filename, recursive=recursive))
                else:
                    # Add current file to the results
                    Log.debug('File: {current_filename}'.format(current_filename=current_filename))
                    files.append(current_filename)
        except Exception as file_list_exception:
            Log.exception(ClientError.ERROR_FILE_LIST_UNHANDLED_EXCEPTION, file_list_exception)

        return files

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
        Log.trace('Uploading File...')

        # Make sure the file doesn't already exist if overwrite is disabled
        if allow_overwrite is False:
            Log.debug('Checking File Does Not Exist At Destination...')
            if self.file_exists(remote_filename=remote_filename) is True:
                raise Exception(ClientError.ERROR_FILE_UPLOAD_EXISTS)

        # Make sure the destination path exists on the server
        Log.debug('Creating Upload Path...')
        self.path_create(os.path.dirname(remote_filename))

        # Upload the file
        try:
            Log.debug('Uploading...')
            self.__connection__.put(localpath=local_filename, remotepath=remote_filename, confirm=confirm, callback=callback)
        except Exception as upload_exception:
            Log.exception(ClientError.ERROR_FILE_UPLOAD_UNHANDLED_EXCEPTION, upload_exception)

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
        Log.trace('Uploading String To File...')

        Log.debug('Creating Temporary File...')
        local_temp_filename = '{local_path}/{filename}'.format(local_path=Helpers.create_unique_local_temp_path(), filename=os.path.basename(remote_filename))
        ClientLocalDisk.file_create_from_string(filename=local_temp_filename, contents=contents)

        # Ensure destination folder exists on SFTP server
        try:
            Log.debug('Checking Destination Folder Exists...')
            self.path_create(os.path.dirname(remote_filename))
        except Exception as folder_exception:
            Log.exception(ClientError.ERROR_FILE_UPLOAD_TEMP_FOLDER, folder_exception)

        # Upload the file to SFTP Server
        try:
            Log.debug('Uploading...')
            self.file_upload(local_filename=local_temp_filename, remote_filename=remote_filename, confirm=confirm, allow_overwrite=allow_overwrite)
        except Exception as upload_exception:
            Log.exception(ClientError.ERROR_FILE_UPLOAD_UNHANDLED_EXCEPTION, upload_exception)

        # Trigger user callback function if there is one
        try:
            if callback is not None:
                Log.debug('Triggering Upload Callback...')
                callback(remote_filename=remote_filename, contents=contents)
        except Exception as callback_exception:
            Log.exception(ClientError.ERROR_FILE_UPLOAD_CALLBACK, callback_exception)

        # Remove the local temporary file we created
        Log.debug('Deleting Temporary file...')
        ClientLocalDisk.file_delete(filename=local_temp_filename)

    def file_delete(self, remote_filename):
        """
        Delete a file from SFTP server

        :type remote_filename: str
        :param remote_filename: Filename/path of the file to download from the SFTP server

        :return: None
        """
        Log.trace('Deleting SFTP File...')

        # Ensure the file exists
        Log.debug('Checking File Exists...')
        if self.file_exists(remote_filename=remote_filename) is False:
            Log.exception(ClientError.ERROR_FILE_DELETE_SOURCE_NOT_FOUND)

        # Delete the file
        try:
            Log.debug('Deleting...')
            self.__connection__.remove(remote_filename)
        except Exception as delete_exception:
            Log.exception('An unexpected error occurred during deletion of file', delete_exception)
            raise delete_exception

        # Ensure the file was deleted
        Log.debug('Checking File Deleted Successfully...')
        if self.file_exists(remote_filename=remote_filename) is True:
            Log.exception(ClientError.ERROR_FILE_DELETE_FAILED)

    def file_move(self, source_filename, destination_filename, allow_overwrite=True) -> None:
        """
        Move file inside SFTP server

        :type source_filename: str
        :param source_filename:

        :type destination_filename: str
        :param destination_filename:

        :type allow_overwrite: bool
        :param allow_overwrite:

        :return: None
        """
        # Ensure the source file exists
        Log.debug('Checking Source File Exists...')
        if self.file_exists(remote_filename=source_filename) is False:
            Log.exception(ClientError.ERROR_FILE_MOVE_SOURCE_NOT_FOUND)

        # If allow overwrite is disabled, make sure the file doesn't already exist
        Log.debug('Checking Destination File Does Not Exist...')
        if self.file_exists(remote_filename=destination_filename) is True:
            if allow_overwrite is False:
                Log.exception(ClientError.ERROR_FILE_MOVE_DESTINATION_EXISTS)
            else:
                # Delete the destination file
                Log.debug('Deleting Existing File...')
                self.file_delete(remote_filename=destination_filename)

        # Rename the file
        Log.debug('Moving File...')
        self.__connection__.rename(remote_src=source_filename, remote_dest=destination_filename)

        # Make sure the file exists at the destination
        if self.file_exists(remote_filename=destination_filename) is False:
            Log.exception(ClientError.ERROR_FILE_MOVE_FAILED)

    def file_exists(self, remote_filename) -> bool:
        """
        Check if file exists

        :type remote_filename: str
        :param remote_filename: Filename/path of the file to check

        :return: bool
        """
        # Sanitize the file path
        remote_filename = Client.sanitize_path(remote_filename)

        Log.trace('Checking File Exists...')

        try:
            return self.__connection__.exists(remotepath=remote_filename)
        except Exception as exists_exception:
            Log.exception(ClientError.ERROR_FILE_LIST_UNHANDLED_EXCEPTION, exists_exception)

    def file_download(self, local_filename, remote_filename, callback=None, allow_overwrite=True) -> None:
        """
        Download a file from SFTP server

        :type local_filename: str
        :param local_filename: Filename/path of the destination on the local filesystem

        :type remote_filename: str
        :param remote_filename: Filename/path of the file to download from the SFTP server

        :type callback: Callable or None
        :param callback: User callback function

        :type allow_overwrite: bool
        :param allow_overwrite: Boolean flag to allow overwriting of local files

        :return: None
        """
        Log.trace('Downloading File To Disk...')
        Log.debug('Local Filename: {local_filename}'.format(local_filename=local_filename))
        Log.debug('Remote Filename: {remote_filename}'.format(remote_filename=remote_filename))

        # Restrict to maximum allowed files if applicable
        if self.__maximum_files__ is not None and self.__count_downloaded__ >= int(self.__maximum_files__):
            Log.debug('Maximum downloaded file limit reached')
            return

        # If allow overwrite is disabled, make sure the file doesn't already exist
        if allow_overwrite is False:
            if os.path.exists(local_filename) is True:
                Log.error(ClientError.ERROR_FILE_DOWNLOAD_DESTINATION_EXISTS)
                raise Exception(ClientError.ERROR_FILE_DOWNLOAD_DESTINATION_EXISTS)

        # Make sure the download path exists locally before we start downloading
        ClientLocalDisk.path_create(path=os.path.dirname(local_filename))

        # Download the file
        try:
            self.__connection__.get(remotepath=remote_filename, localpath=local_filename, preserve_mtime=True)
        except Exception as download_exception:
            Log.exception(ClientError.ERROR_FILE_DOWNLOAD_UNHANDLED_EXCEPTION, download_exception)

        # Track number of file downloads remaining
        if self.__maximum_files__ is not None:
            self.__count_downloaded__ = self.__count_downloaded__ + 1

        # If a callback function was specified, pass it the filename of the downloaded file
        if callback is not None:
            Log.debug('Triggering Callback...')
            callback(local_filename=local_filename, remote_filename=remote_filename)

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
        :param allow_overwrite: Flag indicating the file is allowed to be overwritten if it already exists. If False, and the file exists an base_exception will be thrown

        :return: None
        """
        Log.trace('Starting Recursive Download...')

        # Restrict to maximum allowed files if applicable
        if self.__maximum_files__ is not None and self.__count_downloaded__ >= int(self.__maximum_files__):
            Log.debug('Maximum downloaded file limit reached')
            return

        # List files in current path
        files_found = self.file_list(remote_path=remote_path, recursive=True)

        # Iterate these files
        for current_remote_filename in files_found:
            # The local filename will stored in the same folder structure as on the SFTP server
            current_local_path = Client.sanitize_path(local_path + '/' + os.path.dirname(current_remote_filename))

            # Make sure the current files path exists locally before we start downloading
            ClientLocalDisk.path_create(path=os.path.dirname(current_local_path))

            # Get the destination local filename
            current_local_filename = Client.sanitize_path(current_local_path + '/' + os.path.basename(current_remote_filename))

            # Download the current file
            Log.debug('Downloading: {filename}...'.format(filename=current_remote_filename))
            self.file_download(local_filename=current_local_filename, remote_filename=current_remote_filename, callback=callback, allow_overwrite=allow_overwrite)

    def path_create(self, remote_path) -> None:
        """
        Ensures a directory exists on the SFTP server, creating if it does not already exist

        :type remote_path: str
        :param remote_path: The path to ensure exists

        :return: None
        """
        Log.trace('Creating Path...')

        if self.__connection__.exists(remote_path) is False:
            Log.debug('Creating Path...')
            self.__connection__.makedirs(remote_path)
            Log.debug('Checking Path Creation...')
            if self.__connection__.exists(remote_path) is False:
                Log.exception(ClientError.ERROR_MAKE_PATH_FAILED)

    @staticmethod
    def fingerprint_to_paramiko_key(fingerprint, fingerprint_type):
        """
        Convert and SFTP private key to key suitable for use by underlying paramiko library

        :type fingerprint: str
        :param fingerprint: SFTP server sftp_fingerprint

        :type fingerprint_type: str
        :param fingerprint_type: SFTP server sftp_fingerprint type (e.g. ssh-rsa, ssh-dss)

        :return: paramiko.RSAKey or paramiko.DSSKey or paramiko/ECDSAKey
        """
        Log.trace('Converting sftp_fingerprint to paramiko key...')

        if fingerprint_type == 'ssh-rsa':
            # RSA Key
            Log.debug('Parsing SSH-RSA host sftp_fingerprint...')
            key = paramiko.RSAKey(data=base64.b64decode(fingerprint))
        elif fingerprint_type == 'ssh-dss':
            # DSS Key
            Log.debug('Parsing SSH-DSS host sftp_fingerprint...')
            key = paramiko.DSSKey(data=base64.b64decode(fingerprint))
        elif fingerprint_type in paramiko.ecdsakey.ECDSAKey.supported_key_format_identifiers():
            # ECDSA Key
            Log.debug('Parsing ECDSA ({fingerprint_type}) host sftp_fingerprint...'.format(
                fingerprint_type=fingerprint_type
            ))
            key = paramiko.ECDSAKey(data=base64.b64decode(fingerprint), validate_point=False)
        else:
            # Unknown key type specified
            Log.error('Unknown sftp_fingerprint type specified: {fingerprint_type}'.format(fingerprint_type=fingerprint_type))
            raise Exception(ClientError.ERROR_INVALID_FINGERPRINT_TYPE)

        return key

    @staticmethod
    def validate_sftp_port(port):
        """
        Validate that an acceptable sftp_port number was specified

        :type port: int/str/None
        :param port: SFTP server sftp_port number (must be in range of 0-65535)

        :return: int
        """
        Log.trace('Validating supplied SFTP sftp_port number...')
        # If no sftp_port was specified generate an error
        if not port:
            raise Exception('No SFTP sftp_port number was specified')

        # If the sftp_port number was passed in as a string, make sure it contains digits
        if type(port) is str:
            if not port.isdigit():
                raise Exception('SFTP sftp_port number was not a valid numeric value')

        # Cast value to an integer
        try:
            port = int(port)
        except ValueError:
            raise Exception('SFTP sftp_port number could not be converted to a valid integer value')

        # Ensure sftp_port number is in valid range
        if port < 0 or port > 65535:
            raise Exception(
                'SFTP sftp_port number specified ({port}) was out of range (0-65535)'.format(
                    port=port
                )
            )

        # Return the valid sftp_port number as an integer value
        return port

    @staticmethod
    def validate_sftp_fingerprint_type(fingerprint_type):
        """
        Validate that the specified sftp_fingerprint type was valid

        :type fingerprint_type: str/None
        :param fingerprint_type: SFTP server finger type (e.g. ssh-rsa)

        :return: str
        """
        Log.trace('Validating SFTP host sftp_fingerprint type...')

        # If no type was specified return None
        if not fingerprint_type:
            return None

        # If the sftp_fingerprint type was not a string it cannot be valid
        if type(fingerprint_type) is not str:
            raise Exception('SFTP sftp_fingerprint type was not a valid string')

        # Convert sftp_fingerprint type to lowercase
        fingerprint_type = fingerprint_type.lower()

        # Make sure the key type is one of the acceptable values
        if fingerprint_type not in ['ssh-rsa', 'ssh-dss']:
            if fingerprint_type not in paramiko.ecdsakey.ECDSAKey.supported_key_format_identifiers():
                # If it is an unknown sftp_fingerprint type, generate an error
                raise Exception(
                    'SFTP sftp_fingerprint type ({fingerprint_type}) was not a valid type'.format(
                        fingerprint_type=fingerprint_type
                    )
                )

        # Return the sftp_fingerprint type
        return fingerprint_type

    @staticmethod
    def validate_sftp_username(sftp_username):
        """
        Validate an SFTP sftp_username was specified

        :type sftp_username: str/None
        :param sftp_username: SFTP sftp_username

        :return: str
        """
        Log.trace('Validating supplied SFTP server sftp_username...')

        # If no sftp_username was specified generate an error
        if not sftp_username:
            raise Exception('SFTP sftp_username not specified')

        # If the sftp_fingerprint type was not a string it cannot be valid
        if type(sftp_username) is not str:
            raise Exception('SFTP sftp_username was not a valid string')

        # Return the sftp_username
        return sftp_username

    @staticmethod
    def validate_sftp_private_key(sftp_private_key):
        """
        Validate the specified SFTP private key was valid and return as a suitable paramiko RSA key value

        :type sftp_private_key: str/None
        :param sftp_private_key: SFTP private key used for connection

        :return: paramiko.RSAKey
        """
        Log.trace('Validating supplied SFTP server private key...')

        # If no private key was specified generate an error
        if not sftp_private_key:
            raise Exception('SFTP private key not specified')

        # If the private key was not a string it cannot be valid
        if type(sftp_private_key) is not str:
            raise Exception('SFTP private key was not a valid string')

        # Convert the private key string to a paramiko RSA key that can be used by the underlying libra4y
        sftp_private_key_string_io = StringIO(sftp_private_key)

        return paramiko.RSAKey.from_private_key(sftp_private_key_string_io)

    @staticmethod
    def validate_sftp_fingerprint(fingerprint):
        """
        Validate the specified SFTP sftp_fingerprint was valid

        :type fingerprint: str/None
        :param fingerprint: SFTP sftp_username

        :return: str
        """
        Log.trace('Validating supplied SFTP host sftp_fingerprint...')

        # If no sftp_fingerprint was specified return None
        if not fingerprint:
            return None

        # If sftp_fingerprint is not a string it cannot be valid
        if not fingerprint:
            raise Exception('SFTP sftp_fingerprint not a valid string value')

        # Return the sftp_fingerprint
        return fingerprint

    @staticmethod
    def validate_sftp_address(address):
        """
        Validate the specified SFTP server sftp_address is valid and can be resolved successfully

        :type address: str/None
        :param address: SFTP server hostname/sftp_address

        :return: str
        """
        Log.trace('Validating supplied SFTP server sftp_address...')

        # Validate host sftp_address was supplied
        if not address:
            raise Exception('SFTP server sftp_address was not specified')

        # If sftp_address is not a string it cannot be valid
        if not address:
            raise Exception('SFTP server sftp_address not a valid string value')

        # Validate host sftp_address can be resolved
        try:
            Log.debug('Attempting to resolve server IP sftp_address...')
            result = socket.gethostbyname(address)
            Log.debug('Resolved Address: {result}'.format(result=result))
        except socket.error:
            raise Exception('SFTP server sftp_address ({address}) could not be resolvable'.format(address=address))

        # Return the valid sftp_address
        return address

    @staticmethod
    def sanitize_path(path) -> str:
        """
        Sanitize the supplied path as appropriate for the current filesystem

        :type path: str
        :param path: The path to be sanitized

        :return: str
        """
        path = str(path).strip()

        while '//' in path:
            path = path.replace('//', '/')

        return path
