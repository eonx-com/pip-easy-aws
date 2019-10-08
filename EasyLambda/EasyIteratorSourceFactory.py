#!/usr/bin/env python
# -*- coding: utf-8 -*-

from EasyLambda.Filesystem.S3 import S3
from EasyLambda.Filesystem.Sftp import Sftp
from EasyLambda.Iterator.Source import Source


class EasyIteratorSourceFactory:
    @staticmethod
    def create_s3_source(
            recursive,
            bucket_name,
            base_path='/',
            success_destinations=None,
            failure_destinations=None,
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
        :param recursive: Flag indicating iteration should be performed recursively

        :type delete_on_success: bool
        :param delete_on_success: If True, files in this source will be deleted on successful iterator

        :type delete_on_failure: bool
        :param delete_on_failure: If True, files in this source will be deleted after an error in iteration

        :type success_destinations: list of EasyIteratorDestination or None
        :param success_destinations: If defined, the destination filesystem where each files will be copied following their successful completion

        :type failure_destinations: list of EasyIteratorDestination or None
        :param failure_destinations: If defined, the destination filesystem where each files will be copied following their failure during iteration

        :return: Source
        """
        filesystem_driver = S3(
            bucket_name=bucket_name,
            base_path=base_path
        )

        return Source(
            filesystem_driver=filesystem_driver,
            recursive=recursive,
            success_destinations=success_destinations,
            failure_destinations=failure_destinations,
            delete_on_success=delete_on_success,
            delete_on_failure=delete_on_failure
        )

    @staticmethod
    def create_sftp_source(
            address,
            username,
            delete_on_success,
            delete_on_failure,
            recursive,
            validate_fingerprint=True,
            password=None,
            rsa_private_key=None,
            fingerprint=None,
            fingerprint_type=None,
            port=22,
            base_path='/',
            success_destinations=None,
            failure_destinations=None
    ) -> Source:
        """
        Create a new SFTP file source

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

        :type base_path: str
        :param base_path: Base SFTP file path, files will be recursively iterated from this path

        :type recursive: bool
        :param recursive: Flag indicating iteration should be performed recursively

        :type validate_fingerprint: bool
        :param validate_fingerprint: Flag indicating SFTP server fingerprint should be validated

        :type delete_on_success: bool
        :param delete_on_success: If True, files in this source will be deleted on successful iterator

        :type delete_on_failure: bool
        :param delete_on_failure: If True, files in this source will be deleted after an error in iteration

        :type success_destinations: list of EasyIteratorDestination or None
        :param success_destinations: If defined, the destination filesystem where each files will be copied following their successful completion

        :type failure_destinations: list of EasyIteratorDestination or None
        :param failure_destinations: If defined, the destination filesystem where each files will be copied following their failure during iteration

        :return: EasyIteratorSource
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

        return Source(
            filesystem_driver=filesystem_driver,
            recursive=recursive,
            success_destinations=success_destinations,
            failure_destinations=failure_destinations,
            delete_on_success=delete_on_success,
            delete_on_failure=delete_on_failure
        )
