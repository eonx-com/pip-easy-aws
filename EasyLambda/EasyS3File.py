#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os

from EasyLambda.EasyLog import EasyLog
from EasyLambda.EasyS3Client import EasyS3Client


class EasyS3File:
    def __init__(self, bucket_name, bucket_filename):
        """
        :type bucket_name: str
        :param bucket_name: Name of the bucket the file is located in 

        :type bucket_filename: str
        :param bucket_filename: Path/filename of the file inside the bucket
        """
        EasyLog.trace('Instantiating EasyS3File: {bucket_name}:{bucket_filename}'.format(
            bucket_name=bucket_name,
            bucket_filename=bucket_filename
        ))

        self.__bucket_name__ = bucket_name
        self.__bucket_filename__ = bucket_filename

    def file_exists(self) -> bool:
        """
        Return boolean flag indicating whether the file exists in S3 
        
        :return: bool 
        """
        EasyLog.trace('Checking EasyS3File Exists: {bucket_name}:{bucket_filename}'.format(
            bucket_name=self.__bucket_name__,
            bucket_filename=self.__bucket_filename__
        ))

        return EasyS3Client.file_exists(
            bucket_name=self.__bucket_name__,
            bucket_filename=self.__bucket_filename__
        )

    def file_delete(self) -> bool:
        """
        Delete the file from S3

        :return: bool
        """
        EasyLog.trace('Deleting EasyS3File: {bucket_name}:{bucket_filename}'.format(
            bucket_name=self.__bucket_name__,
            bucket_filename=self.__bucket_filename__
        ))

        return EasyS3Client.file_delete(
            bucket_name=self.__bucket_name__,
            bucket_filename=self.__bucket_filename__
        )

    def file_move_in(self, source_bucket_name, source_bucket_filename) -> None:
        """
        Move a file into this bucket

        :type source_bucket_name: string
        :param source_bucket_name: The bucket the file should be moved from

        :type source_bucket_filename: string
        :param source_bucket_filename: The filename in the source bucket

        :return: None
        """
        EasyLog.trace('Moving EasyS3File In: {bucket_name}:{bucket_filename}'.format(
            bucket_name=self.__bucket_name__,
            bucket_filename=self.__bucket_filename__
        ))

        return EasyS3Client.move_file(
            source_bucket_name=source_bucket_name,
            source_bucket_filename=source_bucket_filename,
            destination_bucket_name=self.__bucket_name__,
            destination_bucket_filename=self.__bucket_filename__
        )

    def file_move_out(self, destination_bucket_name, destination_bucket_filename) -> None:
        """
        Move this file out of this bucket

        :type destination_bucket_name: string
        :param destination_bucket_name: The bucket the file should be moved to

        :type destination_bucket_filename: string
        :param destination_bucket_filename: The destination filename in the destination bucket

        :return: None
        """
        EasyLog.trace('Moving EasyS3File Out: {bucket_name}:{bucket_filename}'.format(
            bucket_name=self.__bucket_name__,
            bucket_filename=self.__bucket_filename__
        ))
        return EasyS3Client.move_file(
            source_bucket_name=self.__bucket_name__,
            source_bucket_filename=self.__bucket_filename__,
            destination_bucket_name=destination_bucket_name,
            destination_bucket_filename=destination_bucket_filename
        )

    def file_copy_in(self, source_bucket_name, source_bucket_filename) -> None:
        """
        Copy file into this bucket

        :type source_bucket_name: string
        :param source_bucket_name: The bucket the file should be copied to

        :type source_bucket_filename: string
        :param source_bucket_filename: The destination filename in the destination bucket

        :return: None
        """
        EasyLog.trace('Copying EasyS3File File In: {bucket_name}:{bucket_filename}'.format(
            bucket_name=self.__bucket_name__,
            bucket_filename=self.__bucket_filename__
        ))

        return EasyS3Client.copy_file(
            source_bucket_name=source_bucket_name,
            source_bucket_filename=source_bucket_filename,
            destination_bucket_name=self.__bucket_name__,
            destination_bucket_filename=self.__bucket_filename__
        )

    def file_copy_out(self, destination_bucket_name, destination_bucket_filename) -> None:
        """
        Copy file out of this bucket

        :type destination_bucket_name: string
        :param destination_bucket_name: The bucket the file should be copied to

        :type destination_bucket_filename: string
        :param destination_bucket_filename: The destination filename in the destination bucket

        :return: None
        """
        EasyLog.trace('Copying EasyS3File File Out: {bucket_name}:{bucket_filename}'.format(
            bucket_name=self.__bucket_name__,
            bucket_filename=self.__bucket_filename__
        ))

        return EasyS3Client.copy_file(
            source_bucket_name=self.__bucket_name__,
            source_bucket_filename=self.__bucket_filename__,
            destination_bucket_name=destination_bucket_name,
            destination_bucket_filename=destination_bucket_filename
        )

    def file_download(self, local_filename) -> None:
        """
        Download the file to local filesystem

        :type local_filename: str
        :param local_filename: Download filename on local filesystem

        :return: None
        """
        EasyLog.trace('Downloading EasyS3File To File: {bucket_name}:{bucket_filename}'.format(
            bucket_name=self.__bucket_name__,
            bucket_filename=self.__bucket_filename__
        ))

        EasyS3Client.file_download(
            bucket_name=self.__bucket_name__,
            bucket_filename=self.__bucket_filename__,
            local_filename=local_filename
        )

    def file_download_to_string(self, encoding='utf-8') -> str:
        """
        Download the file and return it as a string

        :type encoding: string
        :param encoding: The files content_encoding, defaults to UTF-8

        :return: str
        """
        EasyLog.trace('Downloading EasyS3File To String: {bucket_name}:{bucket_filename}'.format(
            bucket_name=self.__bucket_name__,
            bucket_filename=self.__bucket_filename__
        ))

        return EasyS3Client.file_download_to_string(
            bucket_name=self.__bucket_name__,
            bucket_filename=self.__bucket_filename__,
            encoding=encoding
        )

    def file_upload(self, local_filename) -> None:
        """
        Upload a local file over the current file

        :type local_filename: str
        :param local_filename: File on local filesystem to be uploaded

        :return: EasyS3File
        """
        EasyLog.trace('Uploading EasyS3File From File: {bucket_name}:{bucket_filename}'.format(
            bucket_name=self.__bucket_name__,
            bucket_filename=self.__bucket_filename__
        ))

        return EasyS3Client.file_upload(
            bucket_name=self.__bucket_name__,
            bucket_filename=self.__bucket_filename__,
            local_filename=local_filename
        )

    def file_upload_from_string(self, contents, encoding='utf-8') -> None:
        """
        Upload a string over the current file

        :type contents: string
        :param contents: The content to be uploaded to S3

        :type encoding: string
        :param encoding: The strings content_encoding, defaults to UTF-8

        :return: None
        """
        EasyLog.trace('Uploading EasyS3File From String: {bucket_name}:{bucket_filename}'.format(
            bucket_name=self.__bucket_name__,
            bucket_filename=self.__bucket_filename__
        ))

        return EasyS3Client.file_upload_from_string(
            bucket_name=self.__bucket_name__,
            bucket_filename=self.__bucket_filename__,
            contents=contents,
            encoding=encoding
        )

    def file_get_tags(self) -> dict:
        """
        Return a list of this files tags

        :return: dict
        """
        EasyLog.trace('Retrieving EasyS3File Tags: {bucket_name}:{bucket_filename}'.format(
            bucket_name=self.__bucket_name__,
            bucket_filename=self.__bucket_filename__
        ))

        return EasyS3Client.get_file_tags(
            bucket_name=self.__bucket_name__,
            bucket_filename=self.__bucket_filename__
        )

    def file_set_tags(self, tags) -> None:
        """
        Replace all tags on the file

        :type tags: dict
        :param tags: Dictionary of tags to set on the file, this will overwrite all existing tags

        :return: None
        """
        EasyLog.trace('Setting EasyS3File Tags: {bucket_name}:{bucket_filename}'.format(
            bucket_name=self.__bucket_name__,
            bucket_filename=self.__bucket_filename__
        ))
        EasyS3Client.set_file_tags(
            bucket_name=self.__bucket_name__,
            bucket_filename=self.__bucket_filename__,
            tags=tags
        )

    def get_bucket_name(self) -> str:
        """
        Return the name of the the S3 bucket

        :return: str
        """
        return self.__bucket_name__

    def get_bucket_filename(self) -> str:
        """
        Return the full path/filename this object relates to

        :return: str
        """
        return self.__bucket_filename__

    def get_path(self) -> str:
        """
        Return the path of the file this object relates to

        :return: str
        """
        return os.path.dirname(self.__bucket_filename__)

    def get_basename(self) -> str:
        """
        Return the filename this object relates to

        :return: str
        """
        return os.path.basename(self.__bucket_filename__)
