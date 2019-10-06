from EasyLambda.Filesystem.Filesystem import Filesystem


class EasyFilesystem:
    @staticmethod
    def create_s3_filesystem(bucket_name, base_path='/') -> Filesystem:
        """
        Create a new AWS S3 file source

        :type bucket_name: str
        :param bucket_name:

        :type base_path: str
        :param base_path:

        :return:
        """
        return Filesystem({
            'filesystem': Filesystem.FILESYSTEM_S3,
            'bucket_name': bucket_name,
            'base_path': base_path
        })

    @staticmethod
    def create_sftp_filesystem(
            address,
            username,
            password=None,
            rsa_private_key=None,
            fingerprint=None,
            fingerprint_type=None,
            port=22,
            base_path='/'
    ) -> Filesystem:
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

        :return: Filesystem
        """
        return Filesystem({
            'type': Filesystem.FILESYSTEM_SFTP,
            'address': address,
            'port': port,
            'username': username,
            'rsa_private_key': rsa_private_key,
            'password': password,
            'fingerprint': fingerprint,
            'fingerprint_type': fingerprint_type,
            'base_path': base_path
        })
