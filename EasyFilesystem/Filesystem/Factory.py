from EasyFilesystem.Filesystem.S3 import S3
from EasyFilesystem.Filesystem.Sftp import Sftp


class Factory:
    @staticmethod
    def create_s3_filesystem(bucket_name, base_path='') -> S3:
        """
        :type bucket_name: str
        :param bucket_name: The name of the S3 bucket

        :type base_path: str
        :param base_path: The path inside the folder from which files will be referenced relatively

        :return: S3
        """
        return S3(
            bucket_name=bucket_name,
            base_path=base_path
        )

    @staticmethod
    def create_sftp_filesystem(
            address,
            username,
            password=None,
            rsa_private_key=None,
            fingerprint=None,
            fingerprint_type=None,
            validate_fingerprint=True,
            port=22,
            base_path='/'
    ) -> Sftp:
        """
        Instantiate SFTP filesystem

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

        :type validate_fingerprint: bool
        :param validate_fingerprint: Flag indicating whether host key  will be validated on connection

        :return: FilesystemSftp
        """
        return Sftp(
            address=address,
            username=username,
            password=password,
            rsa_private_key=rsa_private_key,
            fingerprint=fingerprint,
            fingerprint_type=fingerprint_type,
            validate_fingerprint=validate_fingerprint,
            port=port,
            base_path=base_path
        )
