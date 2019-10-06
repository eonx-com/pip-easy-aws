from EasyLambda.EasyLog import EasyLog
from EasyLambda.EasySftp import EasySftp


class EasySftpServer:
    def __init__(
            self,
            address,
            username,
            password=None,
            rsa_private_key=None,
            fingerprint=None,
            fingerprint_type=None,
            port=22,
            base_path='',
            host_key_checking=True,
            log_level=1
    ):
        """
        Setup SFTP server

        :type address: str
        :param address: Host server address/IP address

        :type port: int
        :param port: SFTP port number

        :type username: str
        :param username: Username for authentication

        :type rsa_private_key: str
        :param rsa_private_key: Private key for authentication

        :type password: str
        :param password: Password for authentication

        :type fingerprint: str
        :param fingerprint: Host fingerprint

        :type fingerprint_type: str
        :param fingerprint_type: Host fingerprint type

        :type base_path: str
        :param base_path: Base SFTP file path, all uploads/downloads will have this path prepended

        :type log_level: int
        :param log_level: Debug logging level
        """
        # Set logging level
        EasyLog.level = log_level

        # Store server details for later user
        self.address = address
        self.port = port
        self.base_path = base_path
        self.host_key_checking = host_key_checking
        self.username = username
        self.password = password
        self.rsa_private_key = rsa_private_key
        self.fingerprint = fingerprint
        self.fingerprint_type = fingerprint_type

        # Get an SFTP client
        self.sftp_client = EasySftp()

        # Raise warning if host key checking is disabled
        if self.host_key_checking is False:
            EasyLog.warning('Host key checking has been disabled, this may be a security risk...')

    def get_address(self) -> str:
        """
        Get the server address

        :return: str
        """
        return self.address

    def get_username(self) -> str:
        """
        Get the username used to connect

        :return: str
        """
        return self.username

    def get_port(self) -> int:
        """
        Get the server port

        :return: int
        """
        return self.port

    def get_client(self):
        """
        Return the underlying SFTP client

        :return:
        """
        return self.sftp_client

    def get_base_path(self) -> str:
        """
        Return the current base path

        :return: str
        """
        return self.base_path

    def set_base_path(self, base_path) -> None:
        """
        Modify the base path

        :type base_path: str
        :param base_path: Base SFTP file path, all uploads/downloads will have this path prepended

        :return: None
        """
        self.base_path = base_path

    def is_connected(self) -> bool:
        """
        Check if we are still connected to the SFTP server

        :return: bool
        """
        return self.sftp_client.is_connected()

    def connect(self) -> None:
        """
        Connect to SFTP server

        :return: None
        """
        EasyLog.trace('Connecting to server...')

        if self.rsa_private_key is not None:
            # Connect using RSA private key
            self.sftp_client.connect_rsa_private_key(
                address=self.address,
                port=self.port,
                username=self.username,
                rsa_private_key=self.rsa_private_key,
                fingerprint=self.fingerprint,
                fingerprint_type=self.fingerprint_type
            )
        else:
            # Connect using username/password
            return self.sftp_client.connect_password(
                address=self.address,
                port=self.port,
                username=self.username,
                password=self.password,
                fingerprint=self.fingerprint,
                fingerprint_type=self.fingerprint_type
            )

    def disconnect(self) -> None:
        """
        Disconnect from SFTP server

        :return: None
        """
        EasyLog.trace('Disconnecting...')
        self.sftp_client.disconnect()

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
        self.sftp_client.upload_file(
            local_filename=local_filename,
            remote_filename=self.__get_relative_base_path__(remote_filename),
            callback=callback,
            confirm=confirm
        )

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
        self.sftp_client.upload_string(
            contents=contents,
            remote_filename=self.__get_relative_base_path__(remote_filename),
            callback=callback,
            confirm=confirm
        )

    def list_files(self, remote_path, recursive=False) -> list:
        """
        List a list of all files accessible in the filesystem filesystem

        :type remote_path: str
        :param remote_path: The path in the filesystem to list

        :type recursive: bool
        :param recursive: If True the listing will proceed recursively down through all sub-folders

        :return: list
        """
        EasyLog.trace('Listing files...')
        return self.sftp_client.list_files(
            remote_path=remote_path,
            recursive=recursive
        )

    def delete_file(self, remote_filename) -> None:
        """
        Delete a file from SFTP server

        :type remote_filename: str
        :param remote_filename: Filename/path of the file to download from the SFTP server

        :return: None
        """
        EasyLog.trace('Deleting file...')

        self.sftp_client.delete_file(remote_filename)

    def download_file(self, local_filename, remote_filename) -> None:
        """
        Download a file from SFTP server

        :type local_filename: str
        :param local_filename: Filename/path of the destination on the local filesystem

        :type remote_filename: str
        :param remote_filename: Filename/path of the file to download from the SFTP server

        :return: None
        """
        EasyLog.trace('Downloading file...')

        self.sftp_client.download_file(
            local_filename=local_filename,
            remote_filename=self.__get_relative_base_path__(remote_filename)
        )

    def file_exists(self, remote_filename) -> bool:
        """
        Check if file exists

        :type remote_filename: str
        :param remote_filename: Filename/path of the file to check

        :return: bool
        """
        return self.sftp_client.exists(
            remote_filename=remote_filename
        )

    def download_recursive(self, remote_path, local_path, maximum_files=None, callback=None) -> None:
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

        :return: None
        """
        EasyLog.trace('Starting recursive download...')

        self.sftp_client.download_recursive(
            remote_path=self.__get_relative_base_path__(remote_path),
            local_path=local_path,
            maximum_files=maximum_files,
            callback=callback
        )

    def __get_relative_base_path__(self, path) -> str:
        """
        Construct path using the specified base path for the server

        :type path: str
        :param path: The path to mangle

        :return: str
        """
        path = '{base_path}/{path}'.format(
            base_path=self.base_path,
            path=path
        )

        while '//' in path:
            path = path.replace('//', '/')

        return path
