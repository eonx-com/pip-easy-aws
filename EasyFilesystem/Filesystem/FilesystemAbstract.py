#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Abstract filesystem that is used to abstract common tasks across a variety of filesystems

from abc import abstractmethod
from EasyFilesystem import File


class FilesystemAbstract:
    # Error constants
    ERROR_ITERATION_FAILURE = 'An unexpected error occurred during iteration of the current file'
    ERROR_ITERATION_CALLBACK_EXCEPTION = 'The user callback function generated an exception error'
    ERROR_ITERATION_CLEANUP_EXCEPTION = 'An unhandled exception occurred while attempting to cleanup an iterated file'

    ERROR_STAKING_STRATEGY_INVALID = 'The requested staking strategy was not valid'
    ERROR_STAKING_FILE_NOT_FOUND = 'An unexpected error occurred, the file could not be found'

    # Staking strategies
    STRATEGY_IGNORE = 'IGNORE'
    STRATEGY_RENAME = 'RENAME'
    STRATEGY_PROPERTY = 'PROPERTY'

    @abstractmethod
    def file_stake(self, filename, staking_strategy) -> File:
        pass

    @abstractmethod
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

    @abstractmethod
    def file_list(self, path, recursive=False) -> list:
        """
        List files in the filesystem path

        :type path: str
        :param path: Path in the filesystem to list (relative to whatever base path may be defined)

        :type recursive: bool
        :param recursive: Flag indicating the listing should be recursive. If False, sub-folder contents will not be returned

        :return: list
        """
        pass

    @abstractmethod
    def file_exists(self, filename) -> bool:
        """
        Check if file exists in the filesystem

        :type filename: str
        :param filename: The name of the file to check in the filesystem (relative to whatever base path may be defined)

        :return: bool
        """
        pass

    @abstractmethod
    def file_delete(self, filename) -> None:
        """
        Delete a file from the filesystem

        :type filename: str
        :param filename: The name of the file to be deleted from the filesystem (relative to whatever base path may be defined)

        :return: None
        """
        pass

    @abstractmethod
    def file_download(self, filename, local_filename, allow_overwrite=True):
        """
        Download a file from the filesystem to local storage

        :type filename: str
        :param filename: The name of the file in the filesystem to download (relative to whatever base path may be defined)

        :type local_filename: str
        :param local_filename: The destination on the local filesystem where the file will be downloaded to

        :type allow_overwrite: bool
        :param allow_overwrite: Flag indicating the file is allowed to be overwritten if it already exists. If False, and the file exists an exception will be thrown

        :return:
        """
        pass

    @abstractmethod
    def file_upload(self, filename, local_filename, allow_overwrite=True) -> File:
        """
        Upload a file from local storage to the filesystem

        :type filename: str
        :param filename: The destination on the filesystem where the file will be uploaded to  (relative to whatever base path may be defined)

        :type local_filename: str
        :param local_filename: The name of the local filesystem file that will be uploaded

        :type allow_overwrite: bool
        :param allow_overwrite: Flag indicating the file is allowed to be overwritten if it already exists. If False, and the file exists an exception will be thrown

        :return: File
        """
        pass

    @abstractmethod
    def sanitize_path(self, path):
        """
        Sanitize the supplied path as appropriate for the current filesystem

        :type path: str
        :param path: The path to be sanitized

        :return: str
        """
        pass
