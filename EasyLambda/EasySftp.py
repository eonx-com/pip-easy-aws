import base64
import os
import paramiko
import socket
import warnings

from io import StringIO
from EasyLambda.EasyLog import EasyLog
from pysftp import Connection
from pysftp import CnOpts


class EasySftp:
    # Error constants
    ERROR_CONNECTION_FAILED = 'Failed to connect to SFTP server'
    ERROR_UNKNOWN_FINGERPRINT_TYPE = 'The specified fingerprint type was not known'

    def __init__(self):
        """
        Setup SFTP client
        """
        EasyLog.trace('Instantiating SFTP client class...')

        self.connection = None
        self.count_downloaded = 0
        self.host_key_checking = True

    def __del__(self):
        """
        Close any existing SFTP connection on deletion of this object. This is needed to prevent
        Lambda function from generating an error
        """
        EasyLog.trace('Destroying SFTP client class...')

        if self.is_connected():
            # noinspection PyBroadException
            try:
                self.connection.close()
            except Exception:
                # If an exception occurs during the close ignore it
                pass

    def enable_host_key_checking(self) -> None:
        """
        Enable host key checking on connection

        :return: None
        """
        self.host_key_checking = True

    def disable_host_key_checking(self) -> None:
        """
        Disable host key checking on connection

        :return: None
        """
        self.host_key_checking = False

    def get_connection(self) -> Connection:
        """
        Return the underlying connection object

        :return: Connection
        """
        return self.connection

    def connect_rsa_private_key(
            self,
            address,
            port,
            username,
            rsa_private_key,
            password=None,
            fingerprint=None,
            fingerprint_type=None
    ) -> bool:
        """
        Connect to an SFTP server using private key authentication

        :type username: str
        :param username: Username to be used in connection

        :type password: str or None
        :param password: Optional password

        :type address: str
        :param address: SFTP server address/hostname

        :type port: int
        :param port: SFTP server port number

        :type rsa_private_key: str/None
        :param rsa_private_key: RSA private key to be used

        :type fingerprint: str/None
        :param fingerprint: SFTP server fingerprint used to validate server identity. If not specified the known_hosts
            file on the host machine will be used

        :type fingerprint_type: str
        :param fingerprint_type: SFTP server fingerprint type (e.g. ssh-rsa, ssh-dss). This must be one of the
            key types supported by the underlying paramiko library

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
            EasyLog.debug('Host Fingerprint: {fingerprint} ({fingerprint_type})'.format(fingerprint=fingerprint, fingerprint_type=fingerprint_type))

            try:
                self.connection = Connection(
                    EasySftp.validate_sftp_address(address),
                    port=EasySftp.validate_sftp_port(port),
                    private_key=EasySftp.validate_sftp_private_key(rsa_private_key),
                    username=EasySftp.validate_sftp_username(username),
                    password=password,
                    cnopts=self.__get_connection_options__(
                        address=EasySftp.validate_sftp_address(address),
                        fingerprint=EasySftp.validate_sftp_fingerprint(fingerprint),
                        fingerprint_type=EasySftp.validate_sftp_fingerprint_type(fingerprint_type)
                    )
                )
            except Exception as connection_exception:
                EasyLog.exception('Failed to connect to SFTP server', connection_exception)
                raise Exception(EasySftp.ERROR_CONNECTION_FAILED)

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
        Connect to an SFTP server using username and password

        :type username: str
        :param username: Username to be used in connection

        :type address: str
        :param address: SFTP server address/hostname

        :type port: int
        :param port: SFTP server port number

        :type password: str
        :param password: Password for authentication

        :type fingerprint: str/None
        :param fingerprint: SFTP server fingerprint used to validate server identity. If not specified the known_hosts
            file on the host machine will be used

        :type fingerprint_type: str/None
        :param fingerprint_type: SFTP server fingerprint type (e.g. ssh-rsa, ssh-dss). This must be one of the
            key types supported by the underlying paramiko library

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

            self.connection = Connection(
                EasySftp.validate_sftp_address(address),
                port=EasySftp.validate_sftp_port(port),
                username=EasySftp.validate_sftp_username(username),
                password=password,
                cnopts=self.__get_connection_options__(
                    address=EasySftp.validate_sftp_address(address),
                    fingerprint=EasySftp.validate_sftp_fingerprint(fingerprint),
                    fingerprint_type=EasySftp.validate_sftp_fingerprint_type(fingerprint_type)
                )
            )

            # Assert we received the expected warning;
            if -1 in w:
                if issubclass(w[-1].category, UserWarning):
                    assert 'Failed to load HostKeys' in str(w[-1].message)
                    assert 'You will need to explicitly load HostKeys' in str(w[-1].message)

            # Turn warnings back on
            warnings.simplefilter("default")

        EasyLog.debug('Connection successful')
        return True

    def is_connected(self):
        """
        Check if we are still connected to the SFTP server

        :return: bool
        """
        EasyLog.trace('Checking connection state...')
        try:

            # If no connection has every been established return false
            if self.connection is None:
                EasyLog.debug('Not connected (connection not instantiated)')
                return False

            # If there is no SFTP client inside the connection object, return false
            if self.connection.sftp_client is None:
                EasyLog.debug('Not connected (client not instantiated)')
                return False

            # If there is no SFTP channel inside the client object, return false
            if self.connection.sftp_client.get_channel() is None:
                EasyLog.debug('Not connected (no transport channel found)')
                return False

            # If there is no SFTP transport, return false
            if self.connection.sftp_client.get_channel().get_transport() is None:
                EasyLog.debug('Not connected (no transport object found)')
                return False

            # Otherwise return the current state of the underlying transport layer

            if self.connection.sftp_client.get_channel().get_transport().is_active() is True:
                return True

            EasyLog.debug('Not connected (by process of elimination)')
            return False

        except Exception as state_exception:
            # If anything goes wrong, assume we are dead in the water
            EasyLog.warning('Unexpected exception during connection state check: {state_exception}'.format(state_exception=state_exception))
            EasyLog.trace('Not connected (unable to determine connection state)')
            return False

    def disconnect(self):
        """
        Disconnect from SFTP server

        :return: None
        """
        EasyLog.trace('Disconnecting from SFTP server...')
        self.connection.close()

    def assert_remote_path_exists(self, remote_path) -> None:
        """
        Ensure a directory exists on the SFTP server, creating if it does not

        :type remote_path: str
        :param remote_path: The path to test/create

        :return: None
        """
        EasyLog.trace('Checking remote path exists: {remote_path}...'.format(remote_path=remote_path))

        if self.connection.exists(remote_path) is False:
            EasyLog.debug('Destination path not found, creating folder...')
            self.connection.makedirs(remote_path)

    # noinspection PyMethodMayBeStatic
    def assert_local_path_exists(self, local_path) -> None:
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

    def upload_file(self, local_filename, remote_filename, callback=None, confirm=True) -> None:
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
        EasyLog.trace('Uploading file...')

        # Ensure destination folder exists
        self.assert_remote_path_exists(os.path.dirname(remote_filename))

        EasyLog.debug('Starting upload...')
        try:
            self.connection.put(
                localpath=local_filename,
                remotepath=remote_filename,
                confirm=confirm,
                callback=callback
            )
        except Exception as upload_exception:
            EasyLog.exception('An unexpected error occurred during file upload', upload_exception)
            raise upload_exception

        EasyLog.debug('Upload finished')

    def upload_string(self, contents, remote_filename, callback=None, confirm=True) -> None:
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
        EasyLog.trace('Uploading string...')

        # Ensure destination folder exists
        self.assert_remote_path_exists(os.path.dirname(remote_filename))

        try:
            self.connection.putfo(
                StringIO(contents),
                remote_filename,
                confirm=confirm,
                callback=callback
            )
        except Exception as upload_exception:
            EasyLog.exception('An unexpected error occurred during string upload', upload_exception)
            raise upload_exception

        EasyLog.debug('Upload finished')

    def delete_file(self, remote_filename):
        """
        Delete a file from SFTP server

        :type remote_filename: str
        :param remote_filename: Filename/path of the file to download from the SFTP server

        :return: None
        """
        EasyLog.trace('Deleting file: {remote_filename}...'.format(remote_filename=remote_filename))

        try:
            self.connection.remove(remote_filename)
        except Exception as delete_exception:
            EasyLog.exception('An unexpected error occurred during file deletion', delete_exception)
            raise delete_exception

        EasyLog.debug('Deleting completed')

    def exists(self, remote_filename):
        """
        Check if file exists

        :type remote_filename: str
        :param remote_filename: Filename/path of the file to check

        :return: None
        """
        EasyLog.trace('Checking file exists: {remote_filename}...'.format(remote_filename=remote_filename))

        return self.connection.exists(remote_filename)

    def download_file(self, local_filename, remote_filename) -> None:
        """
        Download a file from SFTP server

        :type local_filename: str
        :param local_filename: Filename/path of the destination on the local filesystem

        :type remote_filename: str
        :param remote_filename: Filename/path of the file to download from the SFTP server

        :return: None
        """
        EasyLog.trace('Downloading file: {remote_filename}...'.format(remote_filename=remote_filename))

        try:
            # Make sure the path exists locally
            self.assert_local_path_exists(os.path.dirname(local_filename))

            self.connection.get(
                remotepath=remote_filename,
                localpath=local_filename,
                preserve_mtime=True
            )
        except Exception as download_exception:
            EasyLog.exception('An unexpected error occurred during file download', download_exception)
            raise download_exception

        EasyLog.debug('Download finished')

    def list_files(self, remote_path, recursive=False):
        """
        List a list of all files accessible in the filesystem filesystem

        :type remote_path: str
        :param remote_path: The path in the filesystem to list

        :type recursive: bool
        :param recursive: If True the listing will proceed recursively down through all sub-folders

        :return: list
        """
        EasyLog.trace('Listing files: {remote_path}...'.format(remote_path=remote_path))
        try:
            files = []

            # List files in current remote path
            current_path = self.connection.listdir(remote_path)

            # Iterate through all of the files
            for current_remote_path in current_path:
                current_remote_path = remote_path + '/' + current_remote_path
                current_remote_path = EasySftp.sanitize_path(current_remote_path)

                if self.connection.isdir(current_remote_path) and recursive is True:
                    # If the current path is a directory, recurse down further
                    files.append(self.list_files(
                        remote_path=current_remote_path,
                        recursive=recursive
                    ))
                else:
                    # Append the current file to the list of found files
                    EasyLog.debug('Found: {current_remote_path}'.format(current_remote_path=current_remote_path))
                    files.append(current_remote_path)
        except Exception as list_files_exception:
            EasyLog.exception('An unexpected error occurred during listing of files', list_files_exception)
            raise list_files_exception

        return files

    def download_recursive(self, remote_path, local_path, maximum_files=None, callback=None) -> bool:
        """
        Recursively download all files found in the specified remote path to the specified local path

        :type remote_path: str
        :param remote_path: Path on SFTP server to be downloaded

        :type local_path: str
        :param local_path: Path on local file system where contents are to be downloaded

        :type maximum_files: int/None
        :param maximum_files: Limit the maximum number of files to be downloaded. Process will return early once
            this limit has been reached

        :type callback: function/None
        :param callback: Optional callback function to call after each file has downloaded, must accept a single
            string parameter which contains the local filesystem filename

        :return: bool flag indicating whether to continue recursion
        """
        EasyLog.trace('Starting recursive download...')

        try:
            # Restrict to maximum allowed files if applicable
            if maximum_files is not None and self.count_downloaded >= int(maximum_files):
                EasyLog.debug('Maximum downloaded file limit reached, ending recursion...')
                return False

            EasyLog.debug('Downloading: {remote_path}...'.format(remote_path=remote_path))

            # List files in current path
            current_path = self.connection.listdir(remote_path)

            # Iterate these files
            for current_remote_path in current_path:
                current_remote_path = remote_path + '/' + current_remote_path
                current_remote_path = EasySftp.sanitize_path(current_remote_path)

                if self.connection.isdir(current_remote_path):
                    # If the current path is a directory, recurse down further
                    if self.download_recursive(
                            remote_path=current_remote_path,
                            local_path=local_path,
                            maximum_files=maximum_files,
                            callback=callback
                    ) is False:
                        return False
                else:
                    # Make sure the path exists locally
                    destination_path = EasySftp.sanitize_path(local_path + '/' + os.path.dirname(current_remote_path))
                    self.assert_local_path_exists(destination_path)

                    # Get the filename
                    destination_filename = EasySftp.sanitize_path(destination_path + '/' + os.path.basename(current_remote_path))

                    EasyLog.debug('Downloading: {filename}'.format(filename=current_remote_path))
                    self.connection.get(
                        remotepath=current_remote_path,
                        localpath=destination_filename,
                        preserve_mtime=True
                    )

                    # If a callback function was specified, pass it the filename of the downloaded file
                    if callback is not None:
                        EasyLog.debug('Triggering user callback function...')
                        callback(filename=destination_filename)

                    self.count_downloaded = self.count_downloaded + 1

                    # Restrict to maximum allowed files if applicable
                    if maximum_files is not None and self.count_downloaded >= int(maximum_files):
                        EasyLog.debug('Maximum downloaded file limit reached, ending recursion...')
                        return False

        except Exception as download_exception:
            EasyLog.exception('An unexpected error occurred during recursive download of files', download_exception)
            raise download_exception

        return True

    def __get_connection_options__(self, address, fingerprint, fingerprint_type):
        """
        Get connection settings for pysftp client

        :type address: str
        :param address: SFTP server address/hostname

        :type fingerprint: str/None
        :param fingerprint: SFTP server fingerprint used to validate server identity. If not specified the known_hosts
            file on the host machine will be used

        :type fingerprint_type: str/None
        :param fingerprint_type: SFTP server fingerprint type (e.g. ssh-rsa, ssh-dss). This must be one of the
            key types supported by the underlying paramiko library

        :return: obj
        """
        EasyLog.trace('Retrieving connection options...')
        options = CnOpts()

        if self.host_key_checking is False:
            EasyLog.warning('Host fingerprint checking disabled, this may be a security risk...')
            options.hostkeys = None
        else:
            # If a valid fingerprint and type were specified, add these to the known hosts, otherwise pysftp will use
            # the known_hosts file on the computer
            if fingerprint is not None and fingerprint_type is not None:
                EasyLog.debug('Adding known host fingerprint to client...')
                options.hostkeys.add(
                    hostname=address,
                    keytype=fingerprint_type,
                    key=self.fingerprint_to_paramiko_key(fingerprint, fingerprint_type)
                )
            else:
                EasyLog.warning('No host fingerprints added, relying on known_hosts file')

        return options

    @staticmethod
    def fingerprint_to_paramiko_key(fingerprint, fingerprint_type):
        """
        Convert and SFTP private key to key suitable for use by underlying paramiko library

        :type fingerprint: str
        :param fingerprint: SFTP server fingerprint

        :type fingerprint_type: str
        :param fingerprint_type: SFTP server fingerprint type (e.g. ssh-rsa, ssh-dss)

        :return: paramiko.RSAKey or paramiko.DSSKey or paramiko/ECDSAKey
        """
        EasyLog.trace('Converting fingerprint to paramiko key...')

        if fingerprint_type == 'ssh-rsa':
            # RSA Key
            EasyLog.debug('Parsing SSH-RSA host fingerprint...')
            key = paramiko.RSAKey(data=base64.b64decode(fingerprint))
        elif fingerprint_type == 'ssh-dss':
            # DSS Key
            EasyLog.debug('Parsing SSH-DSS host fingerprint...')
            key = paramiko.DSSKey(data=base64.b64decode(fingerprint))
        elif fingerprint_type in paramiko.ecdsakey.ECDSAKey.supported_key_format_identifiers():
            # ECDSA Key
            EasyLog.debug('Parsing ECDSA ({fingerprint_type}) host fingerprint...'.format(
                fingerprint_type=fingerprint_type
            ))
            key = paramiko.ECDSAKey(data=base64.b64decode(fingerprint), validate_point=False)
        else:
            # Unknown key type specified
            EasyLog.error('Unknown fingerprint type specified: {fingerprint_type}'.format(fingerprint_type=fingerprint_type))
            raise Exception(EasySftp.ERROR_UNKNOWN_FINGERPRINT_TYPE)

        return key

    @staticmethod
    def validate_sftp_port(port):
        """
        Validate that an acceptable port number was specified

        :type port: int/str/None
        :param port: SFTP server port number (must be in range of 0-65535)

        :return: int
        """
        EasyLog.trace('Validating supplied SFTP port number...')
        # If no port was specified generate an error
        if not port:
            raise Exception('No SFTP port number was specified')

        # If the port number was passed in as a string, make sure it contains digits
        if type(port) is str:
            if not port.isdigit():
                raise Exception('SFTP port number was not a valid numeric value')

        # Cast value to an integer
        try:
            port = int(port)
        except ValueError:
            raise Exception('SFTP port number could not be converted to a valid integer value')

        # Ensure port number is in valid range
        if port < 0 or port > 65535:
            raise Exception(
                'SFTP port number specified ({port}) was out of range (0-65535)'.format(
                    port=port
                )
            )

        # Return the valid port number as an integer value
        return port

    @staticmethod
    def validate_sftp_fingerprint_type(fingerprint_type):
        """
        Validate that the specified fingerprint type was valid

        :type fingerprint_type: str/None
        :param fingerprint_type: SFTP server finger type (e.g. ssh-rsa)

        :return: str
        """
        EasyLog.trace('Validating SFTP host fingerprint type...')

        # If no type was specified return None
        if not fingerprint_type:
            return None

        # If the fingerprint type was not a string it cannot be valid
        if type(fingerprint_type) is not str:
            raise Exception('SFTP fingerprint type was not a valid string')

        # Convert fingerprint type to lowercase
        fingerprint_type = fingerprint_type.lower()

        # Make sure the key type is one of the acceptable values
        if fingerprint_type not in ['ssh-rsa', 'ssh-dss']:
            if fingerprint_type not in paramiko.ecdsakey.ECDSAKey.supported_key_format_identifiers():
                # If it is an unknown fingerprint type, generate an error
                raise Exception(
                    'SFTP fingerprint type ({fingerprint_type}) was not a valid type'.format(
                        fingerprint_type=fingerprint_type
                    )
                )

        # Return the fingerprint type
        return fingerprint_type

    @staticmethod
    def validate_sftp_username(sftp_username):
        """
        Validate an SFTP username was specified

        :type sftp_username: str/None
        :param sftp_username: SFTP username

        :return: str
        """
        EasyLog.trace('Validating supplied SFTP server username...')

        # If no username was specified generate an error
        if not sftp_username:
            raise Exception('SFTP username not specified')

        # If the fingerprint type was not a string it cannot be valid
        if type(sftp_username) is not str:
            raise Exception('SFTP username was not a valid string')

        # Return the username
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
        Validate the specified SFTP fingerprint was valid

        :type fingerprint: str/None
        :param fingerprint: SFTP username

        :return: str
        """
        EasyLog.trace('Validating supplied SFTP host fingerprint...')

        # If no fingerprint was specified return None
        if not fingerprint:
            return None

        # If fingerprint is not a string it cannot be valid
        if not fingerprint:
            raise Exception('SFTP fingerprint not a valid string value')

        # Return the fingerprint
        return fingerprint

    @staticmethod
    def validate_sftp_address(address):
        """
        Validate the specified SFTP server address is valid and can be resolved successfully

        :type address: str/None
        :param address: SFTP server hostname/address

        :return: str
        """
        EasyLog.trace('Validating supplied SFTP server address...')

        # Validate host address was supplied
        if not address:
            raise Exception('SFTP server address was not specified')

        # If address is not a string it cannot be valid
        if not address:
            raise Exception('SFTP server address not a valid string value')

        # Validate host address can be resolved
        try:
            EasyLog.debug('Attempting to resolve server IP address...')
            result = socket.gethostbyname(address)
            EasyLog.debug('Resolved Address: {result}'.format(result=result))
        except socket.error:
            raise Exception('SFTP server address ({address}) could not be resolvable'.format(address=address))

        # Return the valid address
        return address

    @staticmethod
    def sanitize_path(value):
        """
        Remove all duplicate slashes from paths

        :param value: Path to be cleaned
        :type value: str

        :return: str
        """
        value = str(value)

        while '//' in value:
            value = value.replace('//', '/')

        return value
