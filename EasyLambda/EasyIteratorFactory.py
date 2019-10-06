from EasyLambda.Iterator.EasyFilesystem import EasyFilesystem
from EasyLambda.Iterator.Destination.Destination import Destination
from EasyLambda.Iterator.Source.Source import Source


class EasyIteratorFactory:
    @staticmethod
    def create_s3_source(
            bucket_name,
            base_path='/',
            delete_on_success=False,
            delete_on_error=False
    ) -> Source:
        """
        Create a new S3 file source

        :type bucket_name: str
        :param bucket_name: The name of the S3 bucket this object

        :type base_path: str
        :param base_path: Base path inside the the bucket

        :type delete_on_success: bool
        :param delete_on_success: If True, files in this source will be deleted on successful iterator

        :type delete_on_error: bool
        :param delete_on_error: If True, files in this source will be deleted after an error in iteration

        :return: Source
        """
        filesystem = EasyFilesystem.create_s3_filesystem(
            bucket_name=bucket_name,
            base_path=base_path
        )

        return Source(
            filesystem=filesystem,
            delete_on_success=delete_on_success,
            delete_on_error=delete_on_error
        )

    @staticmethod
    def create_sftp_source(
            address,
            username,
            password=None,
            rsa_private_key=None,
            fingerprint=None,
            fingerprint_type=None,
            port=22,
            base_path='/',
            delete_on_success=False,
            delete_on_error=False
    ) -> Source:
        """
        Create a new SFTP file source

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
        :param base_path: Base SFTP file path, files will be recursively iterated from this path

        :type delete_on_success: bool
        :param delete_on_success:

        :type delete_on_error: bool
        :param delete_on_error:

        :return: Source
        """
        filesystem = EasyFilesystem.create_sftp_filesystem(
            address=address,
            username=username,
            password=password,
            rsa_private_key=rsa_private_key,
            fingerprint=fingerprint,
            fingerprint_type=fingerprint_type,
            port=port,
            base_path=base_path
        )

        return Source(
            filesystem=filesystem,
            delete_on_success=delete_on_success,
            delete_on_error=delete_on_error
        )

    @staticmethod
    def create_s3_destination(
            bucket_name,
            base_path='/',
            delete_on_success=False,
            delete_on_error=False
    ) -> Destination:
        """
        Create a new AWS S3 file destination

        :type bucket_name: str
        :param bucket_name:

        :type base_path: str
        :param base_path:

        :type delete_on_success: bool
        :param delete_on_success:

        :type delete_on_error: bool
        :param delete_on_error:

        :return: Destination
        """
        filesystem = EasyFilesystem.create_s3_filesystem(
            bucket_name=bucket_name,
            base_path=base_path
        )

        return Destination(
            filesystem=filesystem,
            delete_on_success=delete_on_success,
            delete_on_error=delete_on_error
        )

    @staticmethod
    def create_sftp_destination(
            address,
            username,
            password=None,
            rsa_private_key=None,
            fingerprint=None,
            fingerprint_type=None,
            port=22,
            base_path='/',
            delete_on_success=False,
            delete_on_error=False
    ) -> Destination:
        """
        Create a new SFTP file destination

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
        :param base_path: Base SFTP file path, files will be recursively iterated from this path

        :type delete_on_success: bool
        :param delete_on_success:

        :type delete_on_error: bool
        :param delete_on_error:

        :return: Destination
        """
        filesystem = EasyFilesystem.create_sftp_filesystem(
            address=address,
            username=username,
            password=password,
            rsa_private_key=rsa_private_key,
            fingerprint=fingerprint,
            fingerprint_type=fingerprint_type,
            port=port,
            base_path=base_path
        )

        return Destination(
            filesystem=filesystem,
            delete_on_success=delete_on_success,
            delete_on_error=delete_on_error
        )
