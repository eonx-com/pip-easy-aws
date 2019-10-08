#!/usr/bin/env python
# -*- coding: utf-8 -*-

from EasyLambda.Filesystem.BaseFilesystem import BaseFilesystem
from EasyLambda.EasyLog import EasyLog
from EasyLambda.EasyS3Bucket import EasyS3Bucket


class S3(BaseFilesystem):
    def __init__(
            self,
            bucket_name,
            base_path
    ):
        """
        Instantiate S3 filesystem

        :type bucket_name: str
        :param bucket_name: The S3 bucket name

        :type base_path: str
        :param base_path: The base path in the S3 bucket
        """
        EasyLog.trace('Instantiating S3 filesystem...')

        self.__s3_bucket__ = EasyS3Bucket(
            bucket_name=bucket_name,
            base_path=base_path
        )

    def iterate_files(self, callback, maximum_files, success_destinations, failure_destinations, delete_on_success, delete_on_failure, recursive, staking_strategy) -> list:
        """
        Iterate all files in the filesystem and return the number of files that were iterated

        :type callback: function
        :param callback: User callback function that is executed for each iterated file

        :type maximum_files: int or None
        :param maximum_files: The maximum number of files to iterate

        :type success_destinations: list of EasyIteratorDestination or None
        :param success_destinations: If defined, the destination filesystem where each files will be copied following their successful completion

        :type failure_destinations: list of EasyIteratorDestination or None
        :param failure_destinations: If defined, the destination filesystem where each files will be copied following their failure during iteration

        :type delete_on_success: bool
        :param delete_on_success: If True, files will be deleted from the source on successful iteration

        :type delete_on_failure: bool
        :param delete_on_failure: If True, files will be deleted from the source if an error occurs during iteration

        :type recursive: bool
        :param recursive: Flag indicating iteration should be performed recursively

        :type staking_strategy: str
        :param staking_strategy: The staking strategy to adopt

        :return: list[EasyIteratorStakedFile]
        """
        pass

    def file_list(self, path, recursive=False) -> list:
        """
        List files in the filesystem path

        :type path: str
        :param path: Path in the filesystem to list (relative to whatever base path may be defined)

        :type recursive: bool
        :param recursive: Flag indicating the listing should be recursive. If False, sub-folder contents will not be returned

        :return: list
        """
        return self.__s3_bucket__.file_list(
            bucket_path=path,
            recursive=recursive
        )

    def file_exists(self, filename) -> bool:
        """
        Check if file exists in the filesystem

        :type filename: str
        :param filename: The name of the file to check in the filesystem (relative to whatever base path may be defined)

        :return: bool
        """
        return self.__s3_bucket__.file_exists(
            bucket_filename=filename
        )

    def file_delete(self, filename) -> None:
        """
        Delete a file from the filesystem

        :type filename: str
        :param filename: The name of the file to be deleted from the filesystem (relative to whatever base path may be defined)

        :return: None
        """
        self.__s3_bucket__.file_delete(
            bucket_filename=filename
        )

    def file_download(self, filename, local_filename):
        """
        Download a file from the filesystem to local storage

        :type filename: str
        :param filename: The name of the file in the filesystem to download (relative to whatever base path may be defined)

        :type local_filename: str
        :param local_filename: The destination on the local filesystem where the file will be downloaded to

        :return:
        """
        self.__s3_bucket__.file_download(
            bucket_filename=filename,
            local_filename=local_filename
        )

    def file_upload(self, filename, local_filename):
        """
        Upload a file from local storage to the filesystem

        :type filename: str
        :param filename: The destination on the filesystem where the file will be uploaded to  (relative to whatever base path may be defined)

        :type local_filename: str
        :param local_filename: The name of the local filesystem file that will be uploaded

        :return: None
        """
        self.__s3_bucket__.file_upload(
            bucket_filename=filename,
            local_filename=local_filename
        )
