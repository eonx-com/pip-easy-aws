#!/usr/bin/env python
# -*- coding: utf-8 -*-

from EasyLog.Log import Log
from EasyS3.Client import Client


class Bucket:
    def __init__(self, bucket_name, base_path=''):
        """
        :type bucket_name: str
        :param bucket_name: The name of the S3 bucket this object

        :type base_path: str
        :param base_path: Base path inside the the bucket
        """
        Log.trace('Instantiating S3 Bucket...')

        self.__base_path__ = Client.sanitize_path(path=base_path)
        self.__bucket_name__ = bucket_name

        Log.debug('Bucket Name: {bucket_name}'.format(bucket_name=bucket_name))
        Log.debug('Bucket Base Path: {base_path}'.format(base_path=base_path))

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
        return Client.sanitize_path('{base_path}/{bucket_path}'.format(base_path=self.__base_path__, bucket_path=bucket_filename))

    def file_list(self, bucket_path='', recursive=False) -> list:
        """
        List the content of a path inside this bucket

        :type bucket_path: string
        :param bucket_path: The path inside the bucket (relative to its specified base path)

        :type recursive: bool
        :param recursive: If true all sub-folder of the path will be iterated

        :return: list[str]
        """
        Log.trace('Listing Bucket Files...')

        bucket_path = self.get_path_relative_to_base_path(bucket_filename=bucket_path)

        return Client.file_list(bucket_name=self.__bucket_name__, bucket_path=bucket_path, recursive=recursive)

    def file_exists(self, bucket_filename) -> bool:
        """
        Check if specified file exists in this bucket

        :type bucket_filename: string
        :param bucket_filename: Path/filename to search for in bucket

        :return: bool
        """
        Log.trace('Checking Bucket File Exists...')

        bucket_filename = self.get_path_relative_to_base_path(bucket_filename=bucket_filename)

        return Client.file_exists(bucket_name=self.__bucket_name__, bucket_filename=bucket_filename)

    def file_delete(self, bucket_filename) -> None:
        """
        Delete a file from this bucket

        :type bucket_filename: string
        :param bucket_filename: Path of the file in this bucket to be deleted

        :return: None
        """
        Log.trace('Deleting Bucket File...')

        bucket_filename = self.get_path_relative_to_base_path(bucket_filename=bucket_filename)

        Client.file_delete(bucket_name=self.__bucket_name__, bucket_filename=bucket_filename)

    def file_move_in(self, source_bucket_name, source_bucket_filename, destination_bucket_filename, allow_overwrite=True) -> None:
        """
        Move a file from another bucket into this bucket

        :type source_bucket_name: string
        :param source_bucket_name: The bucket the file should be moved from

        :type source_bucket_filename: string
        :param source_bucket_filename: The filename in the source bucket

        :type destination_bucket_filename: string
        :param destination_bucket_filename: The destination filename in this bucket

        :type allow_overwrite: bool
        :param allow_overwrite: Flag indicating the file is allowed to be overwritten if it already exists. If False, and the file exists an base_exception will be thrown

        :return: None
        """
        Log.trace('Moving File Into Bucket...')

        destination_bucket_filename = self.get_path_relative_to_base_path(bucket_filename=destination_bucket_filename)

        Client.file_move(
            source_bucket_name=source_bucket_name,
            source_bucket_filename=source_bucket_filename,
            destination_bucket_name=self.__bucket_name__,
            destination_bucket_filename=destination_bucket_filename,
            allow_overwrite=allow_overwrite
        )

    def file_move_out(self, source_bucket_filename, destination_bucket_name, destination_bucket_filename, allow_overwrite=True) -> None:
        """
        Move a file out of this bucket to another bucket/path

        :type source_bucket_filename: string
        :param source_bucket_filename: The filename to be moved from this bucket

        :type destination_bucket_name: string
        :param destination_bucket_name: The bucket the file should be moved to

        :type destination_bucket_filename: string
        :param destination_bucket_filename: The destination filename in the destination bucket

        :type allow_overwrite: bool
        :param allow_overwrite: Flag indicating the file is allowed to be overwritten if it already exists. If False, and the file exists an base_exception will be thrown

        :return: None
        """
        Log.trace('Moving File Out Of Bucket...')

        source_bucket_filename = self.get_path_relative_to_base_path(bucket_filename=source_bucket_filename)

        Client.file_move(
            source_bucket_name=self.__bucket_name__,
            source_bucket_filename=source_bucket_filename,
            destination_bucket_name=destination_bucket_name,
            destination_bucket_filename=destination_bucket_filename,
            allow_overwrite=allow_overwrite
        )

    def file_copy_in(self, source_bucket_filename, destination_bucket_name, destination_bucket_filename, allow_overwrite=True) -> None:
        """
        Copy a file from this bucket to another bucket

        :type source_bucket_filename: string
        :param source_bucket_filename: The filename in this bucket to be copied

        :type destination_bucket_name: string
        :param destination_bucket_name: The bucket the file should be copied to

        :type destination_bucket_filename: string
        :param destination_bucket_filename: The filename in the destination bucket

        :type allow_overwrite: bool
        :param allow_overwrite: Flag indicating the file is allowed to be overwritten if it already exists. If False, and the file exists an base_exception will be thrown

        :return: None
        """
        Log.trace('Copying File Into Bucket...')

        source_bucket_filename = self.get_path_relative_to_base_path(bucket_filename=source_bucket_filename)
        destination_bucket_filename = self.get_path_relative_to_base_path(bucket_filename=destination_bucket_filename)

        return Client.file_copy(
            source_bucket_name=self.__bucket_name__,
            source_bucket_filename=source_bucket_filename,
            destination_bucket_name=destination_bucket_name,
            destination_bucket_filename=destination_bucket_filename,
            allow_overwrite=allow_overwrite
        )

    def file_copy_out(self, source_bucket_name, source_bucket_filename, destination_bucket_filename, allow_overwrite=True) -> None:
        """
        Copy a file from another bucket into this one

        :type source_bucket_name: string
        :param source_bucket_name: The bucket the file should be copied from

        :type source_bucket_filename: string
        :param source_bucket_filename: The filename in the source bucket

        :type destination_bucket_filename: string
        :param destination_bucket_filename: The destination filename in this bucket

        :type allow_overwrite: bool
        :param allow_overwrite: Flag indicating the file is allowed to be overwritten if it already exists. If False, and the file exists an base_exception will be thrown

        :return: None
        """
        Log.trace('Copying File Out Of Bucket...')

        destination_bucket_filename = self.get_path_relative_to_base_path(bucket_filename=destination_bucket_filename)

        return Client.file_copy(
            source_bucket_name=source_bucket_name,
            source_bucket_filename=source_bucket_filename,
            destination_bucket_name=self.__bucket_name__,
            destination_bucket_filename=destination_bucket_filename,
            allow_overwrite=allow_overwrite
        )

    def file_download(self, bucket_filename, local_filename, allow_overwrite=True) -> None:
        """
        Download a file from this S3 bucket to local disk

        :type bucket_filename: string
        :param bucket_filename: Path of the file to be downloaded in S3 bucket

        :type local_filename: str
        :param local_filename: Download filename on local filesystem

        :type allow_overwrite: bool
        :param allow_overwrite: Flag indicating the file is allowed to be overwritten if it already exists. If False, and the file exists an base_exception will be thrown

        :return: None
        """
        Log.trace('Downloading File From Bucket...')

        bucket_filename = self.get_path_relative_to_base_path(bucket_filename=bucket_filename)

        Client.file_download(
            bucket_name=self.__bucket_name__,
            bucket_filename=bucket_filename,
            local_filename=local_filename,
            allow_overwrite=allow_overwrite
        )

    def file_upload(self, bucket_filename, local_filename, allow_overwrite=True) -> None:
        """
        Upload a local file to the bucket

        :type bucket_filename: string
        :param bucket_filename: Destination filename in S3 bucket

        :type local_filename: str
        :param local_filename: File on local filesystem to be uploaded

        :type allow_overwrite: bool
        :param allow_overwrite: Flag indicating the file is allowed to be overwritten if it already exists. If False, and the file exists an base_exception will be thrown

        :return: EasyS3File
        """
        Log.trace('Uploading File To Bucket...')

        bucket_filename = self.get_path_relative_to_base_path(bucket_filename=bucket_filename)

        return Client.file_upload(
            bucket_name=self.__bucket_name__,
            bucket_filename=bucket_filename,
            local_filename=local_filename,
            allow_overwrite=allow_overwrite
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
        Log.trace('Downloading String From Bucket...')

        bucket_filename = self.get_path_relative_to_base_path(bucket_filename=bucket_filename)

        return Client.file_download_to_string(
            bucket_name=self.__bucket_name__,
            bucket_filename=bucket_filename,
            encoding=content_encoding
        )

    def file_upload_from_string(self, bucket_filename, content, content_encoding='utf-8', allow_overwrite=True) -> None:
        """
        Upload the contents of a string to the bucket

        :type bucket_filename: string
        :param bucket_filename: Destination filename in S3 bucket

        :type content: string
        :param content: The contents to be uploaded to S3

        :type content_encoding: string
        :param content_encoding: The strings content_encoding, defaults to UTF-8

        :type allow_overwrite: bool
        :param allow_overwrite: Flag indicating the file is allowed to be overwritten if it already exists. If False, and the file exists an base_exception will be thrown

        :return: None
        """
        Log.trace('Uploading String To Bucket...')

        bucket_filename = self.get_path_relative_to_base_path(bucket_filename=bucket_filename)

        return Client.file_upload_from_string(
            bucket_name=self.__bucket_name__,
            bucket_filename=bucket_filename,
            contents=content,
            encoding=content_encoding,
            allow_overwrite=allow_overwrite
        )

    def file_get_tags(self, bucket_filename) -> dict:
        """
        Return a list of tags on the specified file

        :type bucket_filename: string
        :param bucket_filename: Path of the S3 file

        :return: dict
        """
        Log.trace('Retrieving Bucket File Tags...')

        bucket_filename = self.get_path_relative_to_base_path(bucket_filename=bucket_filename)

        return Client.file_get_tags(bucket_name=self.__bucket_name__, bucket_filename=bucket_filename)

    def file_set_tags(self, bucket_filename, tags) -> None:
        """
        Replace all tags on a file in this bucket with those specified

        :type bucket_filename: str
        :param bucket_filename: Name of the file

        :type tags: dict
        :param tags: Dictionary of tags to set on the file, this will overwrite all existing tags

        :return: None
        """
        Log.trace('Setting Bucket File Tags...')

        bucket_filename = self.get_path_relative_to_base_path(bucket_filename=bucket_filename)

        Client.file_set_tags(
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
        Log.trace('Testing Bucket Path Permissions...')

        bucket_path = self.get_path_relative_to_base_path(bucket_filename=bucket_path)

        return Client.test_client_path(bucket_name=self.__bucket_name__, bucket_path=bucket_path)
