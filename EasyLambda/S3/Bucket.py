#!/usr/bin/env python
# -*- coding: utf-8 -*-

from EasyLambda.EasyLog import EasyLog
from EasyLambda.EasyS3 import EasyS3


class Bucket:
    def __init__(self, bucket_name, base_path=''):
        """
        Setup EasyS3Bucket Object Instance

        :type bucket_name: str
        :param bucket_name: The name of the S3 bucket in which the folder is located

        :type base_path: str
        :param base_path: The base path that will be prepended to all supplied paths

        :return: None
        """
        EasyLog.trace('Creating S3 bucket object...')

        self.__bucket_name__ = bucket_name
        self.__base_path__ = EasyS3.sanitize_path(path=base_path)

        EasyLog.debug('Bucket Name: {bucket_name}'.format(bucket_name=self.__bucket_name__))
        EasyLog.debug('Base Path: {path}'.format(path=self.__base_path__))

    def get_relative_path(self, bucket_filename) -> str:
        """
        Return sanitized bucket path relative to the defined base path

        :type bucket_filename: str
        :param bucket_filename: The path you want returned relative to the base path

        :return: str
        """
        return EasyS3.sanitize_path('{base_path}/{bucket_path}'.format(
            base_path=self.__base_path__,
            bucket_path=bucket_filename
        ))

    def list_files(self, bucket_path='', recursive=False) -> list:
        """
        List the contents of a path inside this bucket

        :type bucket_path: string
        :param bucket_path: The buckets path

        :type recursive: bool
        :param recursive: If true all sub-folder of the path will be iterated

        :return: list
        """
        EasyLog.trace('Listing files in this bucket...')

        bucket_path = self.get_relative_path(bucket_filename=bucket_path)

        EasyLog.debug('Bucket Name: {bucket_name}'.format(bucket_name=self.__bucket_name__))
        EasyLog.debug('Bucket Path: {bucket_path}'.format(bucket_path=bucket_path))

        return EasyS3.list_files(
            bucket_name=self.__bucket_name__,
            bucket_path=bucket_path,
            recursive=recursive
        )

    def file_exists(self, bucket_filename) -> bool:
        """
        Check if file exists in this bucket

        :type bucket_filename: string
        :param bucket_filename: Path/filename to search for in bucket

        :return: bool
        """
        EasyLog.trace('Checking if file exists in this bucket...')

        bucket_filename = self.get_relative_path(bucket_filename=bucket_filename)

        EasyLog.debug('Bucket Name: {bucket_name}'.format(bucket_name=self.__bucket_name__))
        EasyLog.debug('Bucket Filename: {bucket_filename}'.format(bucket_filename=bucket_filename))

        return EasyS3.file_exists(
            bucket_name=self.__bucket_name__,
            bucket_filename=bucket_filename
        )

    def delete_file(self, bucket_filename) -> bool:
        """
        Delete a file from this bucket

        :type bucket_filename: string
        :param bucket_filename: Path of the file in this bucket to be deleted

        :return: bool
        """
        EasyLog.trace('Deleting file from this bucket...')

        bucket_filename = self.get_relative_path(bucket_filename=bucket_filename)

        EasyLog.trace('Bucket Name: {bucket_name}'.format(bucket_name=self.__bucket_name__))
        EasyLog.trace('Bucket Filename: {bucket_filename}'.format(bucket_filename=bucket_filename))

        return EasyS3.delete_file(
            bucket_name=self.__bucket_name__,
            bucket_filename=bucket_filename
        )

    def move_file(self, source_bucket_filename, destination_bucket_name, destination_bucket_filename) -> None:
        """
        Move a file from this bucket to another bucket/path

        :type source_bucket_filename: string
        :param source_bucket_filename: The filename to be moved from this bucket

        :type destination_bucket_name: string
        :param destination_bucket_name: The bucket the file should be moved to

        :type destination_bucket_filename: string
        :param destination_bucket_filename: The destination filename in the destination bucket

        :return: None
        """
        EasyLog.trace('Moving file from this bucket...')

        source_bucket_filename = self.get_relative_path(bucket_filename=source_bucket_filename)

        return EasyS3.move_file(
            source_bucket_name=self.__bucket_name__,
            source_bucket_filename=source_bucket_filename,
            destination_bucket_name=destination_bucket_name,
            destination_bucket_filename=destination_bucket_filename
        )

    def move_file_from(self, source_bucket_name, source_bucket_filename, destination_bucket_filename) -> None:
        """
        Move a file from another bucket into this bucket

        :type source_bucket_name: string
        :param source_bucket_name: The bucket the file should be moved from

        :type source_bucket_filename: string
        :param source_bucket_filename: The filename in the source bucket

        :type destination_bucket_filename: string
        :param destination_bucket_filename: The destination filename in this bucket

        :return: None
        """
        EasyLog.trace('Moving file into this bucket...')

        destination_bucket_filename = self.get_relative_path(bucket_filename=destination_bucket_filename)

        EasyLog.trace('Source Bucket Name: {source_bucket_name}'.format(source_bucket_name=self.__bucket_name__))
        EasyLog.trace('Source Bucket Filename: {source_bucket_filename}'.format(source_bucket_filename=source_bucket_filename))
        EasyLog.trace('Destination Bucket Filename: {destination_bucket_filename}'.format(destination_bucket_filename=destination_bucket_filename))

        return EasyS3.move_file(
            source_bucket_name=source_bucket_name,
            source_bucket_filename=source_bucket_filename,
            destination_bucket_name=self.__bucket_name__,
            destination_bucket_filename=destination_bucket_filename
        )

    def copy_file(self, source_bucket_filename, destination_bucket_name, destination_bucket_filename) -> None:
        """
        Copy a file from this bucket to another bucket

        :type source_bucket_filename: string
        :param source_bucket_filename: The filename in this bucket to be copied

        :type destination_bucket_name: string
        :param destination_bucket_name: The bucket the file should be copied to

        :type destination_bucket_filename: string
        :param destination_bucket_filename: The filename in the destination bucket

        :return: None
        """
        EasyLog.trace('Copying file from this bucket...')

        source_bucket_filename = self.get_relative_path(bucket_filename=source_bucket_filename)

        return EasyS3.copy_file(
            source_bucket_name=self.__bucket_name__,
            source_bucket_filename=source_bucket_filename,
            destination_bucket_name=destination_bucket_name,
            destination_bucket_filename=destination_bucket_filename
        )

    def copy_file_from(self, source_bucket_name, source_bucket_filename, destination_bucket_filename) -> None:
        """
        Copy a file from another bucket into this one

        :type source_bucket_name: string
        :param source_bucket_name: The bucket the file should be copied from

        :type source_bucket_filename: string
        :param source_bucket_filename: The filename in the source bucket

        :type destination_bucket_filename: string
        :param destination_bucket_filename: The destination filename in this bucket

        :return: None
        """
        EasyLog.trace('Copying file into this bucket...')

        destination_bucket_filename = self.get_relative_path(bucket_filename=destination_bucket_filename)

        return EasyS3.copy_file(
            source_bucket_name=source_bucket_name,
            source_bucket_filename=source_bucket_filename,
            destination_bucket_name=self.__bucket_name__,
            destination_bucket_filename=destination_bucket_filename
        )

    def download_file(self, bucket_filename, local_filename) -> None:
        """
        Download a file from this S3 bucket to local disk

        :type bucket_filename: string
        :param bucket_filename: Path of the file to be downloaded in S3 bucket

        :type local_filename: str
        :param local_filename: Download filename on local filesystem

        :return: None
        """
        EasyLog.trace('Downloading file from this bucket to local filesystem...')

        bucket_filename = self.get_relative_path(bucket_filename=bucket_filename)

        EasyS3.download_file(
            bucket_name=self.__bucket_name__,
            bucket_filename=bucket_filename,
            local_filename=local_filename
        )

    def download_string(self, bucket_filename, encoding='utf-8') -> str:
        """
        Download a file from the bucket decoding it to a string

        :type bucket_filename: string
        :param bucket_filename: Path of the S3 file to be retrieved

        :type encoding: string
        :param encoding: The files encoding, defaults to UTF-8

        :return: str
        """
        EasyLog.trace('Downloading file from this bucket to string variable...')

        bucket_filename = self.get_relative_path(bucket_filename=bucket_filename)

        return EasyS3.download_string(
            bucket_name=self.__bucket_name__,
            bucket_filename=bucket_filename,
            encoding=encoding
        )

    def upload_file(self, bucket_filename, local_filename) -> None:
        """
        Upload a local file to the bucket

        :type bucket_filename: string
        :param bucket_filename: Destination filename in S3 bucket

        :type local_filename: str
        :param local_filename: File on local filesystem to be uploaded

        :return: EasyS3File
        """
        EasyLog.trace('Uploading file from local filesystem to this bucket...')

        bucket_filename = self.get_relative_path(bucket_filename=bucket_filename)

        return EasyS3.upload_file(
            bucket_name=self.__bucket_name__,
            bucket_filename=bucket_filename,
            local_filename=local_filename
        )

    def upload_string(self, bucket_filename, contents, encoding='utf-8') -> None:
        """
        Upload the contents of a string to the bucket

        :type bucket_filename: string
        :param bucket_filename: Destination filename in S3 bucket

        :type contents: string
        :param contents: The contents to be uploaded to S3

        :type encoding: string
        :param encoding: The strings encoding, defaults to UTF-8

        :return: None
        """
        EasyLog.trace('Uploading string to file in this bucket...')

        bucket_filename = self.get_relative_path(bucket_filename=bucket_filename)

        return EasyS3.upload_string(
            bucket_name=self.__bucket_name__,
            bucket_filename=bucket_filename,
            contents=contents,
            encoding=encoding
        )

    def get_file_tags(self, bucket_filename) -> dict:
        """
        Return a list of tags on the specified file

        :type bucket_filename: string
        :param bucket_filename: Path of the S3 file

        :return: dict
        """
        EasyLog.trace('Retrieving tags for file in this bucket...')

        bucket_filename = self.get_relative_path(bucket_filename=bucket_filename)

        return EasyS3.get_file_tags(
            bucket_name=self.__bucket_name__,
            bucket_filename=bucket_filename
        )

    def set_file_tags(self, bucket_filename, tags) -> None:
        """
        Replace all tags on a file in this bucket with those specified

        :type bucket_filename: str
        :param bucket_filename: Name of the file

        :type tags: dict
        :param tags: Dictionary of tags to set on the file, this will overwrite all existing tags

        :return: None
        """
        EasyLog.trace('Setting AWS S3 tags for file in this bucket...')

        bucket_filename = self.get_relative_path(bucket_filename=bucket_filename)

        EasyS3.set_file_tags(
            bucket_name=self.__bucket_name__,
            bucket_filename=bucket_filename,
            tags=tags
        )

    def test_path_permissions(self, bucket_path='') -> bool:
        """
        Create a test file with known content, upload it to S3, download it again, and compare the contents

        :type bucket_path: str
        :param bucket_path: Path in bucket to create test file

        :return: bool
        """
        EasyLog.trace('Testing path permissions in this bucket...')

        return EasyS3.test_path_permissions(
            bucket_name=self.__bucket_name__,
            bucket_path=bucket_path
        )

    def get_bucket_name(self) -> str:
        """
        Return the name of the the S3 bucket

        :return: str
        """
        return self.__bucket_name__

    def get_base_path(self) -> str:
        """
        Return the base path

        :return: str
        """
        return self.__base_path__
