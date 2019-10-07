from EasyLambda.EasyFilesystemDriver import EasyFilesystemDriver
from EasyLambda.EasyLog import EasyLog
from EasyLambda.EasySftpServer import EasySftpServer


class EasyFilesystemDriverSftp(EasyFilesystemDriver):
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
    ):
        """
        Instantiate SFTP filesystem driver

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
        :param host_key_checking: Flag indicating whether host key  will be validated on connection
        """
        EasyLog.trace('Instantiating SFTP filesystem driver...')

        self.__address__ = address
        self.__port__ = port
        self.__username__ = username
        self.__rsa_private_key__ = rsa_private_key
        self.__password__ = password
        self.__fingerprint__ = fingerprint
        self.__fingerprint_type__ = fingerprint_type
        self.__base_path__ = base_path
        self.__host_key_checking__ = host_key_checking

        self.__sftp_server__ = EasySftpServer(
            address=address,
            username=username,
            password=password,
            rsa_private_key=rsa_private_key,
            fingerprint=fingerprint,
            fingerprint_type=fingerprint_type,
            port=port,
            base_path=base_path,
            host_key_checking=host_key_checking
        )

    def file_list(self, filesystem_path, recursive=False) -> list:
        """
        List files in the filesystem path

        :type filesystem_path: str
        :param filesystem_path: Path in the filesystem to list (relative to whatever base path may be defined)

        :type recursive: bool
        :param recursive: Flag indicating the listing should be recursive. If False, sub-folder contents will not be returned

        :return: list
        """
        return self.__sftp_server__.file_list(
            remote_path=filesystem_path,
            recursive=recursive
        )

    def file_exists(self, filesystem_filename) -> bool:
        """
        Check if file exists in the filesystem

        :type filesystem_filename: str
        :param filesystem_filename: The name of the file to check in the filesystem (relative to whatever base path may be defined)

        :return: bool
        """
        return self.__sftp_server__.file_exists(
            remote_filename=filesystem_filename
        )

    def file_delete(self, filesystem_filename) -> None:
        """
        Delete a file from the filesystem

        :type filesystem_filename: str
        :param filesystem_filename: The name of the file to be deleted from the filesystem (relative to whatever base path may be defined)

        :return: None
        """
        self.__sftp_server__.file_delete(
            remote_filename=filesystem_filename
        )

    def file_download(self, filesystem_filename, local_filename):
        """
        Download a file from the filesystem to local storage

        :type filesystem_filename: str
        :param filesystem_filename: The name of the file in the filesystem to download (relative to whatever base path may be defined)

        :type local_filename: str
        :param local_filename: The destination on the local filesystem where the file will be downloaded to

        :return:
        """
        self.__sftp_server__.file_download(
            remote_filename=filesystem_filename,
            local_filename=local_filename
        )

    def file_upload(self, filesystem_filename, local_filename):
        """
        Upload a file from local storage to the filesystem

        :type filesystem_filename: str
        :param filesystem_filename: The destination on the filesystem where the file will be uploaded to  (relative to whatever base path may be defined)

        :type local_filename: str
        :param local_filename: The name of the local filesystem file that will be uploaded

        :return: None
        """
        self.__sftp_server__.file_upload(
            remote_filename=filesystem_filename,
            local_filename=local_filename
        )
