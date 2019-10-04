import os

from EasyLambda.EasyLog import EasyLog
from EasyLambda.EasyS3 import EasyS3


class File:
    def __init__(self, bucket_name, bucket_filename):
        """
        :type bucket_name: str
        :param bucket_name: Name of the bucket the file is located in 

        :type bucket_filename: str
        :param bucket_filename: Path/filename of the file inside the bucket
        """
        self.__bucket_name__ = bucket_name
        self.__bucket_filename__ = bucket_filename

    def exists(self) -> bool:
        """
        Return boolean flag indicating whether the file exists in S3 
        
        :return: bool 
        """
        EasyLog.trace('Checking if file exists...')

        return EasyS3.file_exists(
            bucket_name=self.__bucket_name__,
            bucket_filename=self.__bucket_filename__
        )

    def delete(self) -> bool:
        """
        Delete the file from S3

        :return: bool
        """
        EasyLog.trace('Deleting file...')

        return EasyS3.delete_file(
            bucket_name=self.__bucket_name__,
            bucket_filename=self.__bucket_filename__
        )

    def move(self, destination_bucket_name, destination_bucket_filename) -> None:
        """
        Move this file to another bucket/path

        :type destination_bucket_name: string
        :param destination_bucket_name: The bucket the file should be moved to

        :type destination_bucket_filename: string
        :param destination_bucket_filename: The destination filename in the destination bucket

        :return: None
        """
        EasyLog.trace('Moving file...')

        return EasyS3.move_file(
            source_bucket_name=self.__bucket_name__,
            source_bucket_filename=self.__bucket_filename__,
            destination_bucket_name=destination_bucket_name,
            destination_bucket_filename=destination_bucket_filename
        )

    def copy(self, destination_bucket_name, destination_bucket_filename) -> None:
        """
        Copy this file to another bucket/path

        :type destination_bucket_name: string
        :param destination_bucket_name: The bucket the file should be copied to

        :type destination_bucket_filename: string
        :param destination_bucket_filename: The destination filename in the destination bucket

        :return: None
        """
        EasyLog.trace('Copying file...')

        return EasyS3.copy_file(
            source_bucket_name=self.__bucket_name__,
            source_bucket_filename=self.__bucket_filename__,
            destination_bucket_name=destination_bucket_name,
            destination_bucket_filename=destination_bucket_filename
        )

    def download(self, local_filename) -> None:
        """
        Download the file to local filesystem

        :type local_filename: str
        :param local_filename: Download filename on local filesystem

        :return: None
        """
        EasyLog.trace('Downloading file to local filesystem...')

        EasyS3.download_file(
            bucket_name=self.__bucket_name__,
            bucket_filename=self.__bucket_filename__,
            local_filename=local_filename
        )

    def download_string(self, encoding='utf-8') -> str:
        """
        Download the file and return it as a string

        :type encoding: string
        :param encoding: The files encoding, defaults to UTF-8

        :return: str
        """
        EasyLog.trace('Downloading file as string...')

        return EasyS3.download_string(
            bucket_name=self.__bucket_name__,
            bucket_filename=self.__bucket_filename__,
            encoding=encoding
        )

    def upload(self, local_filename) -> None:
        """
        Upload a local file over the current file

        :type local_filename: str
        :param local_filename: File on local filesystem to be uploaded

        :return: EasyS3File
        """
        EasyLog.trace('Uploading file...')

        return EasyS3.upload_file(
            bucket_name=self.__bucket_name__,
            bucket_filename=self.__bucket_filename__,
            local_filename=local_filename
        )

    def upload_string(self, contents, encoding='utf-8') -> None:
        """
        Upload a string over the current file

        :type contents: string
        :param contents: The contents to be uploaded to S3

        :type encoding: string
        :param encoding: The strings encoding, defaults to UTF-8

        :return: None
        """
        EasyLog.trace('Uploading string to file...')

        return EasyS3.upload_string(
            bucket_name=self.__bucket_name__,
            bucket_filename=self.__bucket_filename__,
            contents=contents,
            encoding=encoding
        )

    def get_tags(self) -> dict:
        """
        Return a list of this files tags

        :return: dict
        """
        EasyLog.trace('Retrieving file tags...')

        return EasyS3.get_file_tags(
            bucket_name=self.__bucket_name__,
            bucket_filename=self.__bucket_filename__
        )

    def set_tags(self, tags) -> None:
        """
        Replace all tags on the file

        :type tags: dict
        :param tags: Dictionary of tags to set on the file, this will overwrite all existing tags

        :return: None
        """
        EasyLog.trace('Setting file tags...')

        EasyS3.set_file_tags(
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
        Return the name of the the S3 bucket

        :return: str
        """
        return self.__bucket_filename__

    def get_path(self) -> str:
        """
        Return the path of the file

        :return: str
        """
        return os.path.dirname(self.__bucket_filename__)

    def get_basename(self) -> str:
        """
        Return the filename

        :return: str
        """
        return os.path.basename(self.__bucket_filename__)
