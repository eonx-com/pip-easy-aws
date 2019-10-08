#!/usr/bin/env python
# -*- coding: utf-8 -*-

from EasyFilesystem.Filesystem.FilesystemAbstract import FilesystemAbstract
from EasyLog.Log import Log


class Source:
    def __init__(
            self,
            filesystem,
            recursive,
            delete_on_success,
            delete_on_failure,
            staking_strategy=None,
            success_destinations=None,
            failure_destinations=None
    ):
        """
        This should not be called directly, use the SourceFactory methods to create a source filesystem

        :type filesystem: FilesystemAbstract
        :param filesystem: The underlying filesystem this destination is using
        :type staking_strategy: str
        :param staking_strategy: The type of staking strategy to use

        :type recursive: bool
        :param recursive: Flag indicating iteration should be performed recursively

        :type success_destinations: list of EasyIteratorDestination or None
        :param success_destinations: If defined, the destination filesystem where each files will be copied following their successful completion

        :type delete_on_success: bool
        :param delete_on_success: If True, files will be deleted from the source on successful iteration

        :type failure_destinations: list of EasyIteratorDestination or None
        :param failure_destinations: If defined, the destination filesystem where each files will be copied following their failure during iteration

        :type delete_on_failure: bool
        :param delete_on_failure: If True, files will be deleted from the source if an error occurs during iteration
        """
        Log.trace('Initializing EasyIteratorSource class...')

        if staking_strategy is None:
            # If no staking strategy is selected, ignoring unique staking and just assume downloaded files are staked
            staking_strategy = FilesystemAbstract.STRATEGY_IGNORE
        elif staking_strategy not in (FilesystemAbstract.STRATEGY_IGNORE, FilesystemAbstract.STRATEGY_RENAME, FilesystemAbstract.STRATEGY_PROPERTY):
            # Otherwise ensure we received a known strategy
            Log.error('Unknown staking strategy requested: {staking_strategy}'.format(staking_strategy=staking_strategy))
            raise Exception(FilesystemAbstract.ERROR_STAKING_STRATEGY_INVALID)

        self.__filesystem__ = filesystem
        self.__recursive__ = recursive
        self.__success_destinations__ = success_destinations
        self.__failure_destinations__ = failure_destinations
        self.__delete_on_success__ = delete_on_success
        self.__delete_on_failure__ = delete_on_failure
        self.__staking_strategy__ = staking_strategy

    def iterate_files(self, callback, maximum_files=None) -> list:
        """
        Iterate files from the current source

        :type callback: function
        :param callback: User callback function that is executed for each iterated file

        :type maximum_files: int or None
        :param maximum_files: The maximum number of files to iterate

        :return: list[File]
        """
        # Pass iteration down to the specific driver class
        self.__filesystem__: FilesystemAbstract

        return self.__filesystem__.iterate_files(
            callback=callback,
            maximum_files=maximum_files,
            recursive=self.__recursive__,
            success_destinations=self.__success_destinations__,
            failure_destinations=self.__failure_destinations__,
            delete_on_success=self.__delete_on_success__,
            delete_on_failure=self.__delete_on_failure__,
            staking_strategy=self.__staking_strategy__
        )
