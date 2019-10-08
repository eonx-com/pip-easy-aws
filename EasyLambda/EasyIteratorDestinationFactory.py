#!/usr/bin/env python
# -*- coding: utf-8 -*-

from EasyLambda.Filesystem.S3 import S3
from EasyLambda.Filesystem.Sftp import Sftp
from EasyLambda.Iterator.Destination import Destination


class EasyIteratorDestinationFactory:
    @staticmethod
    def create_s3_destination(
            bucket_name,
            base_path='/',
            create_timestamped_folder=False,
            create_logfile_on_completion=False,
            allow_overwrite=False
    ) -> Destination:
        """
        Create a new AWS S3 file destination

        :type bucket_name: str
        :param bucket_name:

        :type base_path: str
        :param base_path:

        :type create_timestamped_folder: bool
        :param create_timestamped_folder:

        :type create_logfile_on_completion: bool
        :param create_logfile_on_completion:

        :type allow_overwrite: bool
        :param allow_overwrite:

        :return: EasyIteratorDestination
        """
        filesystem_driver = S3(
            bucket_name=bucket_name,
            base_path=base_path
        )

        return Destination(
            filesystem_driver=filesystem_driver,
            create_timestamped_folder=create_timestamped_folder,
            create_logfile_on_completion=create_logfile_on_completion,
            allow_overwrite=allow_overwrite
        )

    @staticmethod
    def create_sftp_destination(
            address,
            username,
            password=None,
            rsa_private_key=None,
            fingerprint=None,
            fingerprint_type=None,
            validate_fingerprint=True,
            port=22,
            base_path='/',
            create_timestamped_folder=False,
            create_logfile_on_completion=False,
            allow_overwrite=False
    ) -> Destination:
        """
        Create a new SFTP file destination

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
        :param base_path: Base SFTP file path, files will be recursively iterated from this path

        :type create_timestamped_folder: bool
        :param create_timestamped_folder:

        :type create_logfile_on_completion: bool
        :param create_logfile_on_completion:

        :type allow_overwrite: bool
        :param allow_overwrite:

        :return: EasyIteratorDestination
        """
        filesystem_driver = Sftp(
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

        return Destination(
            filesystem_driver=filesystem_driver,
            create_timestamped_folder=create_timestamped_folder,
            create_logfile_on_completion=create_logfile_on_completion,
            allow_overwrite=allow_overwrite
        )
