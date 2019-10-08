#!/usr/bin/env python
# -*- coding: utf-8 -*-

from EasyFilesystem.Filesystem.S3 import S3
from EasyFilesystem.Filesystem.Sftp import Sftp
from EasyFilesystem.Iterator.Source import Source


class IteratorSourceFactory:
    @staticmethod
    def create_s3_source(
            bucket_name,
            base_path='',
            recursive=False,
            delete_on_success=False,
            delete_on_failure=False
    ) -> Source:
        """
        Create a new S3 file source

        :type bucket_name: str
        :param bucket_name: The name of the S3 bucket this object

        :type base_path: str
        :param base_path: Base path inside the the bucket

        :type recursive: bool
        :param recursive: Boolean flag indicating whether iteration of this source should iterate through sub-folders

        :type delete_on_success: bool
        :param delete_on_success: If True, files in this source will be deleted on successful iterator

        :type delete_on_failure: bool
        :param delete_on_failure: If True, files in this source will be deleted after an error in iteration

        :return: Source
        """
        return Source(
            filesystem=S3(
                bucket_name=bucket_name,
                base_path=base_path
            ),
            recursive=recursive,
            delete_on_success=delete_on_success,
            delete_on_failure=delete_on_failure
        )

    @staticmethod
    def create_sftp_source(
            address,
            username,
            password=None,
            rsa_private_key=None,
            fingerprint=None,
            fingerprint_type=None,
            validate_fingerprint=True,
            port=22,
            base_path='/',
            recursive=False,
            delete_on_success=False,
            delete_on_failure=False
    ) -> Source:
        """
        Create a new SFTP file source

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

        :type recursive: bool
        :param recursive: Boolean flag indicating whether iteration of this source should iterate through sub-folders

        :type delete_on_success: bool
        :param delete_on_success: If True, files in this source will be deleted on successful iterator

        :type delete_on_failure: bool
        :param delete_on_failure: If True, files in this source will be deleted after an error in iteration

        :return: Source
        """
        return Source(
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
            recursive=recursive,
            delete_on_success=delete_on_success,
            delete_on_failure=delete_on_failure
        )
