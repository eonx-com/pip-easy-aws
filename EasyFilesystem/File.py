#!/usr/bin/env python
# -*- coding: utf-8 -*-

# File object that is used to abstract common tasks across a variety of filesystems

import os

from EasyFilesystem.Filesystem.FilesystemAbstract import FilesystemAbstract
from EasyLog.Log import Log
from EasyFilesystem.Helpers import Helpers


class File:
    # Error constants
    ERROR_COPY_STAKED_FILE_NO_DESTINATIONS = 'Unable to copy staked file, no destinations were specified'
    ERROR_COPY_STAKED_FILE_NOT_STAKED = 'Unable to copy staked file to destination, the file is not staked'
    ERROR_STAKING_FILE_NOT_FOUND = 'The file you were attempting to stake could not be found in the filesystem'
    ERROR_STAKING_FILE_UNREADABLE = 'The file had been previously staked, however the local copy of the file is no longer accessible'
    ERROR_STAKING_STRATEGY_INVALID = 'The requested staking strategy was invalid'
    ERROR_STAKING_STRATEGY_UNSUPPORTED = 'The requested staking strategy is not supported by this filesystem'

    def __init__(self, filesystem, filename):
        """
        :type filesystem: FilesystemAbstract or S3 or Sftp
        :param filesystem: The filesystem that contains this file

        :type filename: str
        :param filename: The path/filename to the file on the filesystem
        """
        self.__filesystem__ = filesystem
        self.__filename__ = self.__filesystem__.sanitize_path(filename)
        self.__staking_strategy__ = None
        self.__staking_filename__ = None
        self.__staking_filename_remote__ = None

    def exists(self) -> bool:
        """
        Check if the file exists in the target filesystem

        :return: bool
        """
        # If the file is staked, make sure the remote file exists
        if self.is_staked() is True:
            return self.__filesystem__.file_exists(self.__staking_filename_remote__)

        return self.__filesystem__.file_exists(self.__filename__)

    def delete(self) -> None:
        """
        Delete the file from the target filesystem

        :return: None
        """
        self.__filesystem__.file_delete(filename=self.__filename__)

    def download(self, local_filename, allow_overwrite=True) -> None:
        """
        Download the file to the specified local file

        :type local_filename:
        :param local_filename: The destination for the downloaded file on the local filesystem

        :type allow_overwrite: bool
        :param allow_overwrite: Flag indicating the file is allowed to be overwritten if it already exists. If False, and the file exists an exception will be thrown

        :return: None
        """
        self.__filesystem__.file_download(
            filename=self.__filename__,
            local_filename=local_filename,
            allow_overwrite=allow_overwrite
        )

    def upload(self, local_filename, allow_overwrite=True) -> None:
        """
        Upload the specified local file

        :type local_filename:
        :param local_filename: The local file to be uploaded

        :type allow_overwrite: bool
        :param allow_overwrite: Flag indicating the file is allowed to be overwritten if it already exists. If False, and the file exists an exception will be thrown

        :return: None
        """
        self.__filesystem__.file_upload(
            filename=self.__filename__,
            local_filename=local_filename,
            allow_overwrite=allow_overwrite
        )

    def is_staked(self) -> bool:
        """
        Check if there is staking information for this file and return flag indicating whether the file is staked

        :return: bool
        """
        if self.__staking_strategy__ is None or self.__staking_filename__ is None:
            return False

        if os.path.exists(self.__staking_filename__):
            staked_file = open(self.__staking_filename__, "r")
            staked_file_readable = staked_file.readable()
            staked_file.close()

            if staked_file_readable is True:
                return True

        Log.error(File.ERROR_STAKING_FILE_UNREADABLE)
        raise Exception(File.ERROR_STAKING_FILE_UNREADABLE)

    def stake(self, staking_strategy) -> None:
        """
        Attempt to stake (obtain an exclusive lock) on this file

        :type staking_strategy: str
        :param staking_strategy: The staking strategy to adopt

        :return: None
        """
        Log.trace('Staking file with {staking_strategy} strategy...'.format(staking_strategy=staking_strategy))

        # Stake the file using the nominated strategy
        if staking_strategy == FilesystemAbstract.STRATEGY_IGNORE:
            self.stake_ignore_strategy()
            return

        # All other strategies are unsupported by this filesystem
        Log.error(File.ERROR_STAKING_STRATEGY_UNSUPPORTED)
        raise Exception(File.ERROR_STAKING_STRATEGY_UNSUPPORTED)

    def stake_ignore_strategy(self) -> None:
        """
        Download the file local, and ignore the requirement for staking to be exclusive

        :return: None
        """
        Log.trace('Staking file...')

        # Generate unique filename in local storage to which we will download the file
        staking_filename = self.__filesystem__.sanitize_path('{local_temp_folder}/{filename}'.format(
            filename=self.__filename__,
            local_temp_folder=Helpers.create_unique_local_temp_path(),
        ))

        # Download the file to local storage
        Log.debug('Downloading: {filename}'.format(filename=self.__filename__))
        self.__filesystem__.file_download(
            filename=self.__filename__,
            local_filename=staking_filename
        )

        # Display debug information
        Log.debug('Filesystem Type: {filesystem_type}'.format(filesystem_type=type(self.__filesystem__)))
        Log.debug('Filesystem Filename: {filename}'.format(filename=self.__filename__))
        Log.debug('Staking Filename: {staking_filename}'.format(staking_filename=staking_filename))

        # Store the staking information
        self.__staking_strategy__ = FilesystemAbstract.STRATEGY_IGNORE
        self.__staking_filename__ = staking_filename
        self.__staking_filename_remote__ = staking_filename

    def get_filesystem(self) -> FilesystemAbstract:
        """
        Return the underlying filesystem object

        :return: FilesystemAbstract
        """
        return self.__filesystem__

    def get_filename(self) -> str:
        """
        Return the filename/path to the file in the underlying filesystem

        :return: str
        """
        return self.__filename__

    def get_staking_strategy(self) -> str:
        """
        Return the staking strategy used

        :return: str
        """
        return self.__staking_strategy__

    def get_staking_filename(self) -> str:
        """
        Return path/filename to local copy of staked file

        :return: str
        """
        return self.__staking_filename__

    def copy_staked_file_to_destinations(self, destinations) -> None:
        """
        Copy a staked file to the specified destination filesystem(s)

        :type destinations: list[Destination]
        :param destinations: One or more destinations to which the file should be copied

        :return: None
        """
        Log.trace('Copying staked file to destinations...')

        # Make sure the file has been staked
        if self.is_staked() is False:
            Log.error(File.ERROR_COPY_STAKED_FILE_NOT_STAKED)
            raise Exception(File.ERROR_COPY_STAKED_FILE_NOT_STAKED)

        # Make sure at least one destination was specified
        if len(destinations) == 0:
            Log.error(File.ERROR_COPY_STAKED_FILE_NO_DESTINATIONS)
            raise Exception(File.ERROR_COPY_STAKED_FILE_NO_DESTINATIONS)

        # Start the process
        count_destinations_current = 1
        count_destinations = len(destinations)

        for destination in destinations:
            Log.debug('Copying file to destination {count_destinations_current}/count_destinations}...'.format(
                count_destinations_current=count_destinations_current,
                count_destinations=count_destinations
            ))
            destination.file_upload(local_filename=self.get_staking_filename(), destination_filename=self.get_filename())
