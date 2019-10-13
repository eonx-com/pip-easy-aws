#!/usr/bin/env python
# -*- coding: utf-8 -*-

from EasyFilesystem.Filesystem.S3 import S3
from EasyFilesystem.Filesystem.Sftp import Sftp
from EasyFilesystem.Iterator.Destination import Destination


class IteratorDestinationFactory:
    @staticmethod
    def create_s3_destination(
            bucket_name,
            base_path='/',
            create_timestamped_folder=False,
            create_logfile_on_completion=False,
            allow_overwrite=False
    ) -> Destination:
        """
        Create an S3 file destination

        :type bucket_name: str
        :param bucket_name: The name of the S3 bucket

        :type base_path: str
        :param base_path: The path inside the folder to which files will be save

        :type create_timestamped_folder: bool
        :param create_timestamped_folder: If True, files will be saved to a timestamp folder under the base path

        :type create_logfile_on_completion: bool
        :param create_logfile_on_completion: If True, a copy of the execution log will be saved in the base path after completion

        :type allow_overwrite: bool
        :param allow_overwrite: If True, files will be overwritten if they already exist, otherwise an base_exception will be thrown and the file will be moved to the error destinations (if any)


        :return: Destination
        """
        return Destination(
            filesystem=S3(
                bucket_name=bucket_name,
                base_path=base_path
            ),
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
        Create an SFTP file destination

        :type address: str
        :param address: Host server sftp_address/IP sftp_address

        :type username: str
        :param username: Username for authentication

        :type port: int
        :param port: SFTP port number (defaults to 22)

        :type base_path: str
        :param base_path: Base SFTP file path, all uploads/downloads will have this path prepended

        :type password: str or None
        :param password: Password for authentication

        :type rsa_private_key: str or None
        :param rsa_private_key: Private key for authentication

        :type fingerprint: str or None
        :param fingerprint: Host sftp_fingerprint

        :type fingerprint_type: str or None
        :param fingerprint_type: Host sftp_fingerprint type

        :type validate_fingerprint: bool
        :param validate_fingerprint: Flag indicating SFTP server fingerprint should be validated (default to True)

        :type create_timestamped_folder: bool
        :param create_timestamped_folder: If True, files will be saved to a timestamp folder under the base path

        :type create_logfile_on_completion: bool
        :param create_logfile_on_completion: If True, a copy of the execution log will be saved in the base path after completion

        :type allow_overwrite: bool
        :param allow_overwrite: If True, files will be overwritten if they already exist, otherwise an base_exception will be thrown and the file will be moved to the error destinations (if any)

        :return: Destination
        """
        return Destination(
            filesystem=Sftp(
                address=address,
                username=username,
                password=password,
                rsa_private_key=rsa_private_key,
                fingerprint=fingerprint,
                fingerprint_type=fingerprint_type,
                validate_fingerprint=validate_fingerprint,
                port=port,
                base_path=base_path
            ),
            create_timestamped_folder=create_timestamped_folder,
            create_logfile_on_completion=create_logfile_on_completion,
            allow_overwrite=allow_overwrite
        )
