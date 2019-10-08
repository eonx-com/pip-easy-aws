#!/usr/bin/env python
# -*- coding: utf-8 -*-

import base64
import os
import paramiko
import socket
import warnings

from EasyLambda.EasyHelpers import EasyHelpers
from EasyLambda.EasyLog import EasyLog
from io import StringIO
from pysftp import Connection
from pysftp import CnOpts


class EasySftpClient:
    # Error constants
    ERROR_CONNECTION_FAILED = 'Failed to connect to SFTP server'
    ERROR_INVALID_FINGERPRINT = 'Failed to validate the SFTP server host key/fingerprint'
    ERROR_UNKNOWN_FINGERPRINT_TYPE = 'The specified sftp_fingerprint type was not known'
    ERROR_DOWNLOAD_FILE_EXISTS = 'The file download failed as the file already exists and allow overwrite was not enabled'

    def __init__(self):
        """
        Setup SFTP client
        """
        EasyLog.trace('Instantiating SFTP client class...')

        # Creating variables we will be using later
        self.__connection__ = None

        # Used to enforce maximum number of files limit
        self.__count_downloaded__ = 0

        # No file download limit by default
        self.__maximum_files__ = None

        # Enable host key checking by default
        self.__host_key_checking__ = True

    def __del__(self):
        """
        Close any existing SFTP connection on deletion of this object. This is needed to prevent Lambda function from generating an error on completion
        """
        EasyLog.trace('Cleaning up SFTP client class...')

        if self.is_connected():
            # noinspection PyBroadException
            try:
                EasyLog.debug('Closing connection to SFTP server...')
                self.__connection__.close()
            except Exception:
                # Ignore exceptions at this point
                pass

    def enable_host_key_checking(self) -> None:
        """
        Enable host key checking on connection

        :return: None
        """
        self.__host_key_checking__ = True

    def disable_host_key_checking(self) -> None:
        """
        Disable host key checking on connection

        :return: None
        """
        self.__host_key_checking__ = False

    # Connection management functions

    def connect_rsa_private_key(
            self,
            username,
            address,
            port,
            rsa_private_key,
            password=None,
            fingerprint=None,
            fingerprint_type=None
    ) -> bool:
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

        :return: bool
        """
        EasyLog.trace('Opening SFTP connection (RSA Private Key)...')

        # We need to ignore the warning due to non-existence of known_hosts file in Lambda
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")

            EasyLog.debug('SFTP Server: {username}@{address}:{port}'.format(
                address=address,
                port=port,
                username=username
            ))

            EasyLog.debug('RSA Key Length: {key_length} Bytes'.format(key_length=len(rsa_private_key)))
            if fingerprint is not None:
                EasyLog.debug('Host Fingerprint: {fingerprint}'.format(fingerprint=fingerprint))
            if fingerprint_type is not None:
                EasyLog.debug('Host Fingerprint Type: {fingerprint_type}'.format(fingerprint_type=fingerprint_type))

            try:
                # Validate connection details
                address = EasySftpClient.validate_sftp_address(address)
                port = EasySftpClient.validate_sftp_port(port)
                rsa_private_key = EasySftpClient.validate_sftp_private_key(rsa_private_key)
                username = EasySftpClient.validate_sftp_username(username)
                fingerprint = EasySftpClient.validate_sftp_fingerprint(fingerprint)
                fingerprint_type = EasySftpClient.validate_sftp_fingerprint_type(fingerprint_type)

                # Get connection options for SFTP client
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
                EasyLog.exception('Failed to connect to SFTP server', connection_exception)

                if 'no hostkey for host' in str(connection_exception).lower():
                    raise Exception(EasySftpClient.ERROR_INVALID_FINGERPRINT)

                raise Exception(EasySftpClient.ERROR_CONNECTION_FAILED)

            # Assert we received the expected warning;
            if -1 in w:
                if issubclass(w[-1].category, UserWarning):
                    assert 'Failed to load HostKeys' in str(w[-1].message)
                    assert 'You will need to explicitly load HostKeys' in str(w[-1].message)

            # Turn warnings back on
            warnings.simplefilter("default")

        EasyLog.debug('Connection successful')
        return True

    def connect_password(
            self,
            address,
            port,
            username,
            password,
            fingerprint=None,
            fingerprint_type=None
    ):
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

        :return: bool
        """
        EasyLog.trace('Opening SFTP connection (Password)...')

        # We need to ignore the warning due to non-existence of known_hosts file in Lambda
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")

            EasyLog.debug('SFTP Server: {username}@{address}:{port}'.format(
                address=address,
                port=port,
                username=username
            ))
            EasyLog.debug('Host Fingerprint: {fingerprint} ({fingerprint_type})'.format(
                fingerprint=fingerprint,
                fingerprint_type=fingerprint_type
            ))

            # Validate connection details
            address = EasySftpClient.validate_sftp_address(address)
            port = EasySftpClient.validate_sftp_port(port)
            username = EasySftpClient.validate_sftp_username(username)
            fingerprint = EasySftpClient.validate_sftp_fingerprint(fingerprint)
            fingerprint_type = EasySftpClient.validate_sftp_fingerprint_type(fingerprint_type)

            # Get connection options for SFTP client
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
                EasyLog.exception('Failed to connect to SFTP server', connection_exception)

                if 'no hostkey for host' in str(connection_exception).lower():
                    raise Exception(EasySftpClient.ERROR_INVALID_FINGERPRINT)

                raise Exception(EasySftpClient.ERROR_CONNECTION_FAILED)

            # Assert we received the expected warning;
            if -1 in w:
                if issubclass(w[-1].category, UserWarning):
                    assert 'Failed to load HostKeys' in str(w[-1].message)
                    assert 'You will need to explicitly load HostKeys' in str(w[-1].message)

            # Turn warnings back on
            warnings.simplefilter("default")

        EasyLog.debug('Connection successful')
        return True

    def disconnect(self):
        """
        Disconnect from SFTP server

        :return: None
        """
        EasyLog.trace('Disconnecting from SFTP server...')
        self.__connection__.close()

    def is_connected(self):
        """
        Check if we are still connected to the SFTP server

        :return: bool
        """
        EasyLog.trace('Checking SFTP server connection state...')

        try:
            # If no connection has every been established return false
            if self.__connection__ is None:
                EasyLog.debug('Not connected (connection not instantiated)')
                return False

            # If there is no SFTP client inside the connection object, return false
            if self.__connection__.sftp_client is None:
                EasyLog.debug('Not connected (client not instantiated)')
                return False

            # If there is no SFTP channel inside the client object, return false
            if self.__connection__.sftp_client.get_channel() is None:
                EasyLog.debug('Not connected (no transport channel found)')
                return False

            # If there is no SFTP transport, return false
            if self.__connection__.sftp_client.get_channel().get_transport() is None:
                EasyLog.debug('Not connected (no transport object found)')
                return False

            # Otherwise return the current state of the underlying transport layer

            if self.__connection__.sftp_client.get_channel().get_transport().is_active() is True:
                return True

            EasyLog.debug('Not connected (by process of elimination)')
            return False

        except Exception as state_exception:
            # If anything goes wrong, assume we are dead in the water
            EasyLog.warning('Unexpected exception during connection state check: {state_exception}'.format(state_exception=state_exception))
            EasyLog.trace('Not connected (unable to determine connection state)')
            return False

    # File management functions

    def reset_file_download_counter(self):
        """
        Reset the file download counter used to track maximum number of allowed downloads

        :return:
        """
        self.__count_downloaded__ = 0

    def set_file_download_limit(self, maximum_files) -> None:
        """
        Set a limit on the maximum number of downloads allowed

        :type maximum_files int
        :param maximum_files: The maximum number of files that can be downloaded. Once reached any download requested will be skipped

        :return: None
        """
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
        EasyLog.trace('Listing files on SFTP server...')

        try:
            files = []

            # List files in current remote path
            EasyLog.debug('Path: {remote_path}'.format(remote_path=remote_path))
            current_path = self.__connection__.listdir(remote_path)

            # Iterate through all of the files
            for current_remote_path in current_path:
                EasyLog.debug('Current file: {current_remote_path}'.format(current_remote_path=current_remote_path))

                # Sanitize the path
                current_remote_path = EasyHelpers.sanitize_path(remote_path + '/' + current_remote_path)

                # If the current path is a directory we may continue recursing down the folder structure
                if self.__connection__.isdir(current_remote_path):
                    # If we are performing a recursive list, iterate down further
                    if recursive is True:
                        # Down the rabbit hole
                        EasyLog.debug('Recursing sub-folder: {current_remote_path}'.format(current_remote_path=current_remote_path))
                        files.extend(self.file_list(remote_path=current_remote_path, recursive=recursive))
                else:
                    # Append the current file to the list of found files
                    files.append(current_remote_path)
        except Exception as file_list_exception:
            EasyLog.exception('An unexpected error occurred during listing of files', file_list_exception)
            raise file_list_exception

        # Return any files we found
        return files

    def file_upload(self, local_filename, remote_filename, callback=None, confirm=False) -> None:
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

        :return: None
        """
        EasyLog.trace('Uploading file to SFTP server...')

        # Ensure destination folder exists
        self.assert_remote_path_exists(os.path.dirname(remote_filename))

        EasyLog.debug('Uploading: {remote_filename}...'.format(remote_filename=remote_filename))
        try:
            self.__connection__.put(
                localpath=local_filename,
                remotepath=remote_filename,
                confirm=confirm,
                callback=callback
            )
        except Exception as upload_exception:
            EasyLog.exception('An unexpected error occurred during upload of file from local filesystem', upload_exception)
            raise upload_exception

        EasyLog.debug('Upload finished')

    def file_upload_from_string(self, contents, remote_filename, callback=None, confirm=False) -> None:
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

        :return: None
        """
        EasyLog.trace('Uploading file (from string) to SFTP server...')

        # Ensure destination folder exists
        self.assert_remote_path_exists(os.path.dirname(remote_filename))

        try:
            EasyLog.debug('File size: {size}'.format(size=len(contents)))

            # Save the string to the local disk in a temporary file
            local_path = EasyHelpers.create_unique_local_temp_path()
            local_temp_filename = '{local_path}/{filename}'.format(
                local_path=local_path,
                filename=os.path.basename(remote_filename)
            )
            EasyLog.debug('Creating temporary file: {local_temp_filename}'.format(local_temp_filename=local_temp_filename))

            local_temp_file = open(local_temp_filename, "wt")
            local_temp_file.write(contents)
            local_temp_file.close()

            # Upload the file
            EasyLog.debug('Uploading local temporary file to SFTP server...')
            self.file_upload(
                local_filename=local_temp_filename,
                remote_filename=remote_filename,
                confirm=confirm
            )

            # Remove the local temporary file we created
            EasyLog.debug('Deleting local temporary file...')
            os.unlink(local_temp_filename)

            if callback is not None:
                EasyLog.debug('Triggering user callback function...')
                callback(remote_filename=remote_filename, contents=contents)

        except Exception as upload_exception:
            EasyLog.exception('An unexpected error occurred during upload of file from string', upload_exception)
            raise upload_exception

        EasyLog.debug('Upload finished')

    def file_delete(self, remote_filename):
        """
        Delete a file from SFTP server

        :type remote_filename: str
        :param remote_filename: Filename/path of the file to download from the SFTP server

        :return: None
        """
        EasyLog.trace('Deleting file from SFTP server...')

        try:
            self.__connection__.remove(remote_filename)
        except Exception as delete_exception:
            EasyLog.exception('An unexpected error occurred during deletion of file', delete_exception)
            raise delete_exception

        EasyLog.debug('Deleting completed')

    def file_exists(self, remote_filename):
        """
        Check if file exists

        :type remote_filename: str
        :param remote_filename: Filename/path of the file to check

        :return: None
        """
        EasyLog.trace('Checking file exists on SFTP server...')

        try:
            return self.__connection__.exists(remotepath=remote_filename)
        except Exception as exists_exception:
            EasyLog.exception('An unexpected error occurred during check of file existence', exists_exception)
            raise exists_exception

    def file_download(self, local_filename, remote_filename, allow_overwrite=True) -> bool:
        """
        Download a file from SFTP server

        :type local_filename: str
        :param local_filename: Filename/path of the destination on the local filesystem

        :type remote_filename: str
        :param remote_filename: Filename/path of the file to download from the SFTP server

        :type allow_overwrite: bool
        :param allow_overwrite: Boolean flag to allow overwriting of local files

        :return: bool
        """
        EasyLog.trace('Downloading file from SFTP server: {remote_filename}...'.format(remote_filename=remote_filename))

        try:
            # Restrict to maximum allowed files if applicable
            if self.__maximum_files__ is not None and self.__count_downloaded__ >= int(self.__maximum_files__):
                EasyLog.debug('Maximum downloaded file limit reached')
                return False

            # Make sure the path exists locally
            EasySftpClient.assert_local_path_exists(os.path.dirname(local_filename))

            # If allow overwrite is disabled, make sure the file doesn't already exist
            if allow_overwrite is False:
                if os.path.exists(local_filename) is True:
                    EasyLog.error(EasySftpClient.ERROR_DOWNLOAD_FILE_EXISTS)
                    raise Exception(EasySftpClient.ERROR_DOWNLOAD_FILE_EXISTS)

            self.__connection__.get(
                remotepath=remote_filename,
                localpath=local_filename,
                preserve_mtime=True
            )

            if self.__maximum_files__ is not None:
                self.__count_downloaded__ = self.__count_downloaded__ + 1
        except Exception as download_exception:
            EasyLog.exception('An unexpected error occurred during download of file to local filesystem', download_exception)
            raise download_exception

        EasyLog.debug('Download finished')
        return True

    def file_download_recursive(self, remote_path, local_path, callback=None) -> None:
        """
        Recursively download all files found in the specified remote path to the specified local path

        :type remote_path: str
        :param remote_path: Path on SFTP server to be downloaded

        :type local_path: str
        :param local_path: Path on local file system where contents are to be downloaded

        :type callback: function/None
        :param callback: Optional callback function to call after each file has downloaded successfully

        :return: None
        """
        EasyLog.trace('Starting recursive download from SFTP server...')

        try:
            # Restrict to maximum allowed files if applicable
            if self.__maximum_files__ is not None and self.__count_downloaded__ >= int(self.__maximum_files__):
                EasyLog.debug('Maximum downloaded file limit reached, ending recursion...')
                return

            # List files in current path
            files_found = self.file_list(remote_path=remote_path, recursive=True)

            # Iterate these files
            for current_remote_filename in files_found:
                # The local filename will stored in the same folder structure as on the SFTP server
                current_local_path = EasyHelpers.sanitize_path(local_path + '/' + os.path.dirname(current_remote_filename))

                # Make sure the file path exists locally before we download
                EasySftpClient.assert_local_path_exists(current_local_path)

                # Get the filename
                current_local_filename = EasyHelpers.sanitize_path(current_local_path + '/' + os.path.basename(current_remote_filename))

                EasyLog.debug('Downloading: {filename}'.format(filename=current_remote_filename))
                try:
                    if self.file_download(local_filename=current_local_filename, remote_filename=current_remote_filename) is True:
                        # If a callback function was specified, pass it the filename of the downloaded file
                        if callback is not None:
                            EasyLog.debug('Triggering user callback function...')
                            callback(local_filename=current_local_filename, remote_filename=current_remote_filename)
                except Exception as download_exception:
                    if self.file_exists(remote_filename=current_remote_filename) is True:
                        EasyLog.exception('Failed to download file that exists on SFTP server: {current_remote_filename}'.format(current_remote_filename=current_remote_filename), download_exception)
                        raise download_exception

                    EasyLog.warning('Failed to download file from SFTP server, file no longer exists')

        except Exception as download_exception:
            EasyLog.exception('An unexpected error occurred during recursive download of files', download_exception)
            raise download_exception

    def __get_connection_options__(self, address, fingerprint, fingerprint_type):
        """
        Get connection settings for pysftp client

        :type address: str
        :param address: SFTP server sftp_address/hostname

        :type fingerprint: str/None
        :param fingerprint: SFTP server sftp_fingerprint used to validate server identity. If not specified the known_hosts
            file on the host machine will be used

        :type fingerprint_type: str/None
        :param fingerprint_type: SFTP server sftp_fingerprint type (e.g. ssh-rsa, ssh-dss). This must be one of the
            key types supported by the underlying paramiko library

        :return: obj
        """
        EasyLog.trace('Retrieving connection options...')
        options = CnOpts()

        if self.__host_key_checking__ is False:
            EasyLog.warning('Host sftp_fingerprint checking disabled, this may be a security risk...')
            options.hostkeys = None
        else:
            # If a valid sftp_fingerprint and type were specified, add these to the known hosts, otherwise pysftp will use
            # the known_hosts file on the computer
            if fingerprint is not None and fingerprint_type is not None:
                EasyLog.debug('Adding known host sftp_fingerprint to client...')
                options.hostkeys.add(
                    hostname=address,
                    keytype=fingerprint_type,
                    key=self.fingerprint_to_paramiko_key(fingerprint, fingerprint_type)
                )
            else:
                EasyLog.warning('No host fingerprints added, relying on known_hosts file')

        return options

    def assert_remote_path_exists(self, remote_path) -> None:
        """
        Ensure a directory exists on the SFTP server, creating if it does not

        :type remote_path: str
        :param remote_path: The path to test/create

        :return: None
        """
        EasyLog.trace('Checking remote path exists: {remote_path}...'.format(remote_path=remote_path))

        self.__connection__: Connection
        if self.__connection__.exists(remote_path) is False:
            EasyLog.debug('Destination path not found, creating folder...')
            self.__connection__.makedirs(remote_path)

    @staticmethod
    def assert_local_path_exists(local_path) -> None:
        """
        Ensure a directory exists on the local filesystem, creating if it doesn't exist

        :type local_path: str
        :param local_path: The path to test/create

        :return: None
        """
        EasyLog.trace('Checking local path exists: {local_path}...'.format(local_path=local_path))

        if os.path.exists(local_path) is False:
            EasyLog.debug('Destination path not found, creating folder...')
            os.makedirs(local_path)

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
        EasyLog.trace('Converting sftp_fingerprint to paramiko key...')

        if fingerprint_type == 'ssh-rsa':
            # RSA Key
            EasyLog.debug('Parsing SSH-RSA host sftp_fingerprint...')
            key = paramiko.RSAKey(data=base64.b64decode(fingerprint))
        elif fingerprint_type == 'ssh-dss':
            # DSS Key
            EasyLog.debug('Parsing SSH-DSS host sftp_fingerprint...')
            key = paramiko.DSSKey(data=base64.b64decode(fingerprint))
        elif fingerprint_type in paramiko.ecdsakey.ECDSAKey.supported_key_format_identifiers():
            # ECDSA Key
            EasyLog.debug('Parsing ECDSA ({fingerprint_type}) host sftp_fingerprint...'.format(
                fingerprint_type=fingerprint_type
            ))
            key = paramiko.ECDSAKey(data=base64.b64decode(fingerprint), validate_point=False)
        else:
            # Unknown key type specified
            EasyLog.error('Unknown sftp_fingerprint type specified: {fingerprint_type}'.format(fingerprint_type=fingerprint_type))
            raise Exception(EasySftpClient.ERROR_UNKNOWN_FINGERPRINT_TYPE)

        return key

    @staticmethod
    def validate_sftp_port(port):
        """
        Validate that an acceptable sftp_port number was specified

        :type port: int/str/None
        :param port: SFTP server sftp_port number (must be in range of 0-65535)

        :return: int
        """
        EasyLog.trace('Validating supplied SFTP sftp_port number...')
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
        EasyLog.trace('Validating SFTP host sftp_fingerprint type...')

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
        EasyLog.trace('Validating supplied SFTP server sftp_username...')

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
        EasyLog.trace('Validating supplied SFTP server private key...')

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
        EasyLog.trace('Validating supplied SFTP host sftp_fingerprint...')

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
        EasyLog.trace('Validating supplied SFTP server sftp_address...')

        # Validate host sftp_address was supplied
        if not address:
            raise Exception('SFTP server sftp_address was not specified')

        # If sftp_address is not a string it cannot be valid
        if not address:
            raise Exception('SFTP server sftp_address not a valid string value')

        # Validate host sftp_address can be resolved
        try:
            EasyLog.debug('Attempting to resolve server IP sftp_address...')
            result = socket.gethostbyname(address)
            EasyLog.debug('Resolved Address: {result}'.format(result=result))
        except socket.error:
            raise Exception('SFTP server sftp_address ({address}) could not be resolvable'.format(address=address))

        # Return the valid sftp_address
        return address
