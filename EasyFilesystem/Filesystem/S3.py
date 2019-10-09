#!/usr/bin/env python
# -*- coding: utf-8 -*-

from EasyFilesystem.Filesystem.FilesystemAbstract import FilesystemAbstract
from EasyFilesystem.File import File
from EasyS3.Client import Client
from EasyS3.Bucket import Bucket
from EasyFilesystem.Iterator import Destination
from EasyLog.Log import Log


# noinspection DuplicatedCode
class S3(FilesystemAbstract):
    def __init__(
            self,
            bucket_name,
            base_path='/'
    ):
        """
        :type bucket_name: str
        :param bucket_name: S3 bucket name

        :type base_path: str
        :param base_path: Base S3 file path, all uploads/downloads will have this path prepended
        """
        Log.trace('Instantiating S3 filesystem...')

        self.__s3_bucket__ = Bucket(
            bucket_name=bucket_name,
            base_path=base_path
        )

    def iterate_files(self, callback, maximum_files, delete_on_success, delete_on_failure, recursive, staking_strategy, success_destinations=None, failure_destinations=None) -> list:
        """
        Iterate all files in the filesystem and return the number of files that were iterated

        :type callback: function
        :param callback: User callback function that is executed for each iterated file

        :type maximum_files: int or None
        :param maximum_files: The maximum number of files to iterate

        :type success_destinations: list[Destination] or None
        :param success_destinations: If defined, the destination filesystem where each files will be copied following their successful completion

        :type failure_destinations: list[Destination] or None
        :param failure_destinations: If defined, the destination filesystem where each files will be copied following their failure during iteration

        :type delete_on_success: bool
        :param delete_on_success: If True, files will be deleted from the source on successful iteration

        :type delete_on_failure: bool
        :param delete_on_failure: If True, files will be deleted from the source if an error occurs during iteration

        :type recursive: bool
        :param recursive: Flag indicating iteration should be performed recursively

        :type staking_strategy: str
        :param staking_strategy: The staking strategy to adopt

        :return: list[File]
        """
        Log.trace('Iterating files in S3 filesystem...')

        failure_destinations = failure_destinations or []
        success_destinations = success_destinations or []

        filesystem_files = self.file_list(recursive=recursive)
        filesystem_files_iterated = []
        filesystem_files_remaining = maximum_files

        Log.debug('Iterator located {count_files} file(s)...'.format(count_files=len(filesystem_files)))
        if maximum_files is not None:
            Log.debug('Iterating maximum of {maximum_files} files'.format(maximum_files=maximum_files))

        filesystem_file: File
        for filesystem_file in filesystem_files:
            # If we have reached the iteration limit, exit out of the loop
            if filesystem_files_remaining <= 0:
                Log.debug('Iteration limit reached, ending iteration early...')
                break

            # Attempt to stake the current file
            try:
                Log.debug('Attempting to stake file: {filename}...'.format(filename=filesystem_file.get_filename()))
                filesystem_file.stake(staking_strategy)
                filesystem_files_iterated.append(filesystem_file)
            except Exception as staking_exception:
                # If something goes wrong during staking we will continue on as if nothing happened, presumably another process staked it before us
                Log.warning('Staking Failed: {staking_exception}'.format(staking_exception=staking_exception))
                filesystem_files_remaining -= 1
                continue

            # If the file was successfully staked, trigger the user callback
            try:
                Log.debug('Triggering user callback...')
                iteration_success = callback(filesystem_file=filesystem_file)
            except Exception as callback_exception:
                # The user callback generated an exception error, report an exception error- but allow fallthrough so it gets cleaned up
                Log.exception(FilesystemAbstract.ERROR_ITERATION_CALLBACK_EXCEPTION, callback_exception)
                iteration_success = False

            try:
                # Cleanup the current file
                if iteration_success is True and success_destinations is not None:
                    # The user callback failed- move to failure destinations (if any)
                    if len(success_destinations) > 0:
                        Log.debug('Moving failed file to success destination(s)...')
                        filesystem_file.copy_staked_file_to_destinations(destinations=success_destinations)
                elif iteration_success is False and failure_destinations is not None:
                    # The user callback failed- move to failure destinations (if any)
                    if len(failure_destinations) > 0:
                        Log.debug('Moving failed file to failure destination(s)...')
                        filesystem_file.copy_staked_file_to_destinations(destinations=failure_destinations)
            except Exception as cleanup_exception:
                Log.exception(FilesystemAbstract.ERROR_ITERATION_CLEANUP_EXCEPTION, cleanup_exception)
                raise Exception(FilesystemAbstract.ERROR_ITERATION_CLEANUP_EXCEPTION)

            # If all went well, count down the number of files we are allowed to iterate and continue on our way
            filesystem_files_remaining -= 1

        # Return list of all files we iterated
        return filesystem_files_iterated

    def file_list(self, path='/', recursive=False) -> list:
        """
        List files in the filesystem path

        :type path: str
        :param path: Path in the filesystem to list (relative to whatever base path may be defined)

        :type recursive: bool
        :param recursive: Flag indicating the listing should be recursive. If False, sub-folder contents will not be returned

        :return: list[File]
        """
        filenames = self.__s3_bucket__.file_list(
            bucket_path=path,
            recursive=recursive
        )

        filesystem_files = []

        for filename in filenames:
            filesystem_files.append(File(
                filesystem=self,
                filename=filename
            ))

        return filesystem_files

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

    def file_download(self, filename, local_filename, allow_overwrite=True) -> None:
        """
        Download a file from the filesystem to local storage

        :type filename: str
        :param filename: The name of the file in the filesystem to download (relative to whatever base path may be defined)

        :type local_filename: str
        :param local_filename: The destination on the local filesystem where the file will be downloaded to

        :type allow_overwrite: bool
        :param allow_overwrite: Flag indicating the file is allowed to be overwritten if it already exists. If False, and the file exists an exception will be thrown

        :return: None
        """
        self.__s3_bucket__.file_download(
            bucket_filename=filename,
            local_filename=local_filename,
            allow_overwrite=allow_overwrite
        )

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
        self.__s3_bucket__.file_upload(
            bucket_filename=filename,
            local_filename=local_filename,
            allow_overwrite=allow_overwrite
        )

        return File(
            filesystem=self,
            filename=filename
        )

    def sanitize_path(self, path) -> str:
        """
        Sanitize the supplied path as appropriate for the current filesystem

        :type path: str
        :param path: The path to be sanitized

        :return: str
        """
        return Client.sanitize_path(path)

    def file_stake(self, filename, staking_strategy) -> File:
        """
        Attempt to stake (obtain an exclusive lock) on a file in this filesystem

        :type filename: str
        :param filename: The path/filename to be staked

        :type staking_strategy: str
        :param staking_strategy: The staking strategy to adopt

        :return: File
        """
        # Make sure the staking strategy is known
        if staking_strategy not in (
                File.STRATEGY_IGNORE,
                File.STRATEGY_PROPERTY,
                File.STRATEGY_RENAME
        ):
            # Unknown staking strategy
            Log.error(File.ERROR_STAKING_STRATEGY_INVALID)
            raise Exception(File.ERROR_STAKING_STRATEGY_INVALID)

        # Make sure the file exists
        if self.file_exists(filename) is False:
            Log.error(File.ERROR_STAKING_FILE_NOT_FOUND)
            raise Exception(File.ERROR_STAKING_FILE_NOT_FOUND)

        # Create a file object
        filesystem_file = File(filesystem=self, filename=filename)

        # Stake the file
        filesystem_file.stake(staking_strategy=staking_strategy)

        # Return it
        return filesystem_file
