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
            host_key_checking=True
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

        :type host_key_checking: bool
        :param host_key_checking: Flag indicating whether the servers host fingerprint will be verified on connection
        """
        EasyLog.trace('Instantiating SFTP server class...')

        # Store server details
        self.__address__ = address
        self.__port__ = port
        self.__base_path__ = base_path
        self.__host_key_checking__ = host_key_checking
        self.__username__ = username
        self.__password__ = password
        self.__rsa_private_key__ = rsa_private_key
        self.__fingerprint__ = fingerprint
        self.__fingerprint_type__ = fingerprint_type
        self.__host_key_checking__ = host_key_checking

        # Get an EasySftp client
        self.__sftp_client__ = EasySftp()

        # Raise warning if host key checking is disabled
        if self.__host_key_checking__ is False:
            EasyLog.warning('Host key checking has been disabled, this may be a security risk...')
    # Functions to retrieve non-sensitive information

    def get_address(self) -> str:
        """
        Get the server address

        :return: str
        """
        return self.__address__

    def get_username(self) -> str:
        """
        Get the username used to connect

        :return: str
        """
        return self.__username__

    def get_port(self) -> int:
        """
        Get the server port

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
        EasyLog.trace('Connecting to SFTP server...')

        if self.__rsa_private_key__ is not None:
            # Connect using RSA private key
            return self.__sftp_client__.connect_rsa_private_key(
                address=self.__address__,
                port=self.__port__,
                username=self.__username__,
                rsa_private_key=self.__rsa_private_key__,
                fingerprint=self.__fingerprint__,
                fingerprint_type=self.__fingerprint_type__
            )
        else:
            # Connect using username/password
            return self.__sftp_client__.connect_password(
                address=self.__address__,
                port=self.__port__,
                username=self.__username__,
                password=self.__password__,
                fingerprint=self.__fingerprint__,
                fingerprint_type=self.__fingerprint_type__
            )

    def disconnect(self) -> None:
        """
        Disconnect from SFTP server

        :return: None
        """
        EasyLog.trace('Disconnecting from SFTP server...')

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
        EasyLog.trace('Retrieving file list from SFTP server...')

        return self.__sftp_client__.file_list(
            remote_path=remote_path,
            recursive=recursive
        )

    def file_exists(self, remote_filename) -> bool:
        """
        Check if file exists

        :type remote_filename: str
        :param remote_filename: Filename/path of the file to check

        :return: bool
        """
        EasyLog.trace('Checking file exists on SFTP server...')

        return self.__sftp_client__.file_exists(
            remote_filename=remote_filename
        )

    def file_delete(self, remote_filename) -> None:
        """
        Delete a file from SFTP server

        :type remote_filename: str
        :param remote_filename: Filename/path of the file to download from the SFTP server

        :return: None
        """
        EasyLog.trace('Deleting file from SFTP server...')

        self.__sftp_client__.file_delete(remote_filename)

    def file_download(self, local_filename, remote_filename) -> None:
        """
        Download a file from SFTP server

        :type local_filename: str
        :param local_filename: Filename/path of the destination on the local filesystem

        :type remote_filename: str
        :param remote_filename: Filename/path of the file to download from the SFTP server

        :return: None
        """
        EasyLog.trace('Downloading file from SFTP server...')

        self.__sftp_client__.file_download(
            local_filename=local_filename,
            remote_filename=self.__get_relative_base_path__(remote_filename)
        )

    def file_download_recursive(self, remote_path, local_path, maximum_files=None, callback=None) -> None:
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
        EasyLog.trace('Starting recursive download from SFTP server...')

        self.__sftp_client__.file_download_recursive(
            remote_path=self.__get_relative_base_path__(remote_path),
            local_path=local_path,
            maximum_files=maximum_files,
            callback=callback
        )

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

        self.__sftp_client__.file_upload(
            local_filename=local_filename,
            remote_filename=self.__get_relative_base_path__(remote_filename),
            callback=callback,
            confirm=confirm
        )

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

        self.__sftp_client__.file_upload_from_string(
            contents=contents,
            remote_filename=self.__get_relative_base_path__(remote_filename),
            callback=callback,
            confirm=confirm
        )

    # Internal helper methods

    def __get_relative_base_path__(self, path) -> str:
        """
        Construct path using the specified base path for the server

        :type path: str
        :param path: The path to mangle

        :return: str
        """
        path = '{base_path}/{path}'.format(
            base_path=self.__base_path__,
            path=path
        )

        while '//' in path:
            path = path.replace('//', '/')

        return path
