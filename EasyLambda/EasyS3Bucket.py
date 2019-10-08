#!/usr/bin/env python
# -*- coding: utf-8 -*-

from EasyLambda.EasyLog import EasyLog
from EasyLambda.EasyS3Client import EasyS3Client


class EasyS3Bucket:
    def __init__(self, bucket_name, base_path=''):
        """
        :type bucket_name: str
        :param bucket_name: The name of the S3 bucket this object

        :type base_path: str
        :param base_path: Base path inside the the bucket
        """
        self.__bucket_name__ = bucket_name
        self.__base_path__ = EasyS3Client.sanitize_path(path=base_path)

        EasyLog.trace('Instantiating EasyS3Bucket: {bucket_name}:{base_path}'.format(
            bucket_name=self.__bucket_name__,
            base_path=self.__base_path__
        ))

        EasyLog.debug('Bucket Name: {bucket_name}'.format(bucket_name=self.__bucket_name__))
        EasyLog.debug('Base Path: {path}'.format(path=self.__base_path__))

    def get_bucket_name(self) -> str:
        """
        Return the name of this bucket

        :return: str
        """
        return self.__bucket_name__

    def get_base_path(self) -> str:
        """
        Return the base path of this bucket

        :return: str
        """
        return self.__base_path__

    def get_path_relative_to_base_path(self, bucket_filename) -> str:
        """
        Return the full path to the specified file inside this bucket

        :type bucket_filename: str
        :param bucket_filename: The path to the file relative to the buckets base path

        :return: str
        """
        return EasyS3Client.sanitize_path('{base_path}/{bucket_path}'.format(
            base_path=self.__base_path__,
            bucket_path=bucket_filename
        ))

    def file_list(self, bucket_path='', recursive=False) -> list:
        """
        List the content of a path inside this bucket

        :type bucket_path: string
        :param bucket_path: The path inside the bucket (relative to its specified base path)

        :type recursive: bool
        :param recursive: If true all sub-folder of the path will be iterated

        :return: list
        """
        EasyLog.trace('Listing EasyS3Bucket Files: {bucket_name}:{base_path}'.format(
            bucket_name=self.__bucket_name__,
            base_path=self.__base_path__
        ))

        bucket_path = self.get_path_relative_to_base_path(bucket_filename=bucket_path)

        EasyLog.debug('Bucket Name: {bucket_name}'.format(bucket_name=self.__bucket_name__))
        EasyLog.debug('Bucket Path: {bucket_path}'.format(bucket_path=bucket_path))

        return EasyS3Client.file_list(
            bucket_name=self.__bucket_name__,
            bucket_path=bucket_path,
            recursive=recursive
        )

    def file_exists(self, bucket_filename) -> bool:
        """
        Check if specified file exists in this bucket

        :type bucket_filename: string
        :param bucket_filename: Path/filename to search for in bucket

        :return: bool
        """
        EasyLog.trace('Checking EasyS3Bucket File Exists: {bucket_name}:{base_path}'.format(
            bucket_name=self.__bucket_name__,
            base_path=self.__base_path__
        ))

        bucket_filename = self.get_path_relative_to_base_path(bucket_filename=bucket_filename)

        EasyLog.debug('Bucket Name: {bucket_name}'.format(bucket_name=self.__bucket_name__))
        EasyLog.debug('Bucket Filename: {bucket_filename}'.format(bucket_filename=bucket_filename))

        return EasyS3Client.file_exists(
            bucket_name=self.__bucket_name__,
            bucket_filename=bucket_filename
        )

    def file_delete(self, bucket_filename) -> bool:
        """
        Delete a file from this bucket

        :type bucket_filename: string
        :param bucket_filename: Path of the file in this bucket to be deleted

        :return: bool
        """
        EasyLog.trace('Deleting EasyS3Bucket File: {bucket_name}:{base_path}'.format(
            bucket_name=self.__bucket_name__,
            base_path=self.__base_path__
        ))

        bucket_filename = self.get_path_relative_to_base_path(bucket_filename=bucket_filename)

        EasyLog.trace('Bucket Name: {bucket_name}'.format(bucket_name=self.__bucket_name__))
        EasyLog.trace('Bucket Filename: {bucket_filename}'.format(bucket_filename=bucket_filename))

        return EasyS3Client.file_delete(
            bucket_name=self.__bucket_name__,
            bucket_filename=bucket_filename
        )

    def file_move_in(self, source_bucket_name, source_bucket_filename, destination_bucket_filename) -> None:
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
        EasyLog.trace('Moving EasyS3Bucket File In: {bucket_name}:{base_path}'.format(
            bucket_name=self.__bucket_name__,
            base_path=self.__base_path__
        ))

        destination_bucket_filename = self.get_path_relative_to_base_path(bucket_filename=destination_bucket_filename)

        return EasyS3Client.move_file(
            source_bucket_name=source_bucket_name,
            source_bucket_filename=source_bucket_filename,
            destination_bucket_name=self.__bucket_name__,
            destination_bucket_filename=destination_bucket_filename
        )

    def file_move_out(self, source_bucket_filename, destination_bucket_name, destination_bucket_filename) -> None:
        """
        Move a file out of this bucket to another bucket/path

        :type source_bucket_filename: string
        :param source_bucket_filename: The filename to be moved from this bucket

        :type destination_bucket_name: string
        :param destination_bucket_name: The bucket the file should be moved to

        :type destination_bucket_filename: string
        :param destination_bucket_filename: The destination filename in the destination bucket

        :return: None
        """
        EasyLog.trace('Moving EasyS3Bucket File Out: {bucket_name}:{base_path}'.format(
            bucket_name=self.__bucket_name__,
            base_path=self.__base_path__
        ))

        source_bucket_filename = self.get_path_relative_to_base_path(bucket_filename=source_bucket_filename)

        return EasyS3Client.move_file(
            source_bucket_name=self.__bucket_name__,
            source_bucket_filename=source_bucket_filename,
            destination_bucket_name=destination_bucket_name,
            destination_bucket_filename=destination_bucket_filename
        )

    def file_copy_in(self, source_bucket_filename, destination_bucket_name, destination_bucket_filename) -> None:
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
        EasyLog.trace('Copying EasyS3Bucket File In: {bucket_name}:{base_path}'.format(
            bucket_name=self.__bucket_name__,
            base_path=self.__base_path__
        ))

        source_bucket_filename = self.get_path_relative_to_base_path(bucket_filename=source_bucket_filename)

        return EasyS3Client.copy_file(
            source_bucket_name=self.__bucket_name__,
            source_bucket_filename=source_bucket_filename,
            destination_bucket_name=destination_bucket_name,
            destination_bucket_filename=destination_bucket_filename
        )

    def file_copy_out(self, source_bucket_name, source_bucket_filename, destination_bucket_filename) -> None:
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
        EasyLog.trace('Copying EasyS3Bucket File Out: {bucket_name}:{base_path}'.format(
            bucket_name=self.__bucket_name__,
            base_path=self.__base_path__
        ))

        destination_bucket_filename = self.get_path_relative_to_base_path(bucket_filename=destination_bucket_filename)

        return EasyS3Client.copy_file(
            source_bucket_name=source_bucket_name,
            source_bucket_filename=source_bucket_filename,
            destination_bucket_name=self.__bucket_name__,
            destination_bucket_filename=destination_bucket_filename
        )

    def file_download(self, bucket_filename, local_filename) -> None:
        """
        Download a file from this S3 bucket to local disk

        :type bucket_filename: string
        :param bucket_filename: Path of the file to be downloaded in S3 bucket

        :type local_filename: str
        :param local_filename: Download filename on local filesystem

        :return: None
        """
        EasyLog.trace('Downloading File From EasyS3Bucket: {bucket_name}:{base_path}'.format(
            bucket_name=self.__bucket_name__,
            base_path=self.__base_path__
        ))

        bucket_filename = self.get_path_relative_to_base_path(bucket_filename=bucket_filename)

        EasyS3Client.file_download(
            bucket_name=self.__bucket_name__,
            bucket_filename=bucket_filename,
            local_filename=local_filename
        )

    def file_upload(self, bucket_filename, local_filename) -> None:
        """
        Upload a local file to the bucket

        :type bucket_filename: string
        :param bucket_filename: Destination filename in S3 bucket

        :type local_filename: str
        :param local_filename: File on local filesystem to be uploaded

        :return: EasyS3File
        """
        EasyLog.trace('Uploading File To EasyS3Bucket: {bucket_name}:{base_path}'.format(
            bucket_name=self.__bucket_name__,
            base_path=self.__base_path__
        ))

        bucket_filename = self.get_path_relative_to_base_path(bucket_filename=bucket_filename)

        return EasyS3Client.file_upload(
            bucket_name=self.__bucket_name__,
            bucket_filename=bucket_filename,
            local_filename=local_filename
        )

    def file_download_to_string(self, bucket_filename, content_encoding='utf-8') -> str:
        """
        Download a file from the bucket decoding it to a string

        :type bucket_filename: string
        :param bucket_filename: Path of the S3 file to be retrieved

        :type content_encoding: string
        :param content_encoding: The files content_encoding, defaults to UTF-8

        :return: str
        """
        EasyLog.trace('Downloading String From EasyS3Bucket: {bucket_name}:{base_path}'.format(
            bucket_name=self.__bucket_name__,
            base_path=self.__base_path__
        ))

        bucket_filename = self.get_path_relative_to_base_path(bucket_filename=bucket_filename)

        return EasyS3Client.file_download_to_string(
            bucket_name=self.__bucket_name__,
            bucket_filename=bucket_filename,
            encoding=content_encoding
        )

    def file_upload_from_string(self, bucket_filename, content, content_encoding='utf-8') -> None:
        """
        Upload the contents of a string to the bucket

        :type bucket_filename: string
        :param bucket_filename: Destination filename in S3 bucket

        :type content: string
        :param content: The contents to be uploaded to S3

        :type content_encoding: string
        :param content_encoding: The strings content_encoding, defaults to UTF-8

        :return: None
        """
        EasyLog.trace('Uploading String To EasyS3Bucket: {bucket_name}:{base_path}'.format(
            bucket_name=self.__bucket_name__,
            base_path=self.__base_path__
        ))

        bucket_filename = self.get_path_relative_to_base_path(bucket_filename=bucket_filename)

        return EasyS3Client.file_upload_from_string(
            bucket_name=self.__bucket_name__,
            bucket_filename=bucket_filename,
            contents=content,
            encoding=content_encoding
        )

    def file_get_tags(self, bucket_filename) -> dict:
        """
        Return a list of tags on the specified file

        :type bucket_filename: string
        :param bucket_filename: Path of the S3 file

        :return: dict
        """
        EasyLog.trace('Retrieving EasyS3Bucket File Tags: {bucket_name}:{base_path}'.format(
            bucket_name=self.__bucket_name__,
            base_path=self.__base_path__
        ))

        bucket_filename = self.get_path_relative_to_base_path(bucket_filename=bucket_filename)

        return EasyS3Client.get_file_tags(
            bucket_name=self.__bucket_name__,
            bucket_filename=bucket_filename
        )

    def file_set_tags(self, bucket_filename, tags) -> None:
        """
        Replace all tags on a file in this bucket with those specified

        :type bucket_filename: str
        :param bucket_filename: Name of the file

        :type tags: dict
        :param tags: Dictionary of tags to set on the file, this will overwrite all existing tags

        :return: None
        """
        EasyLog.trace('Setting EasyS3Bucket File Tags: {bucket_name}:{base_path}'.format(
            bucket_name=self.__bucket_name__,
            base_path=self.__base_path__
        ))

        bucket_filename = self.get_path_relative_to_base_path(bucket_filename=bucket_filename)

        EasyS3Client.set_file_tags(
            bucket_name=self.__bucket_name__,
            bucket_filename=bucket_filename,
            tags=tags
        )

    def test_bucket_path(self, bucket_path='') -> bool:
        """
        Create a test file with known content, upload it to S3, download it again, and compare the content

        :type bucket_path: str
        :param bucket_path: Path in bucket to create test file

        :return: bool
        """
        EasyLog.trace('Testing EasyS3Bucket Path Permissions: {bucket_name}:{base_path}'.format(
            bucket_name=self.__bucket_name__,
            base_path=self.__base_path__
        ))

        return EasyS3Client.test_path_permissions(
            bucket_name=self.__bucket_name__,
            bucket_path=bucket_path
        )
