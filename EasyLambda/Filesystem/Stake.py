#!/usr/bin/env python
# -*- coding: utf-8 -*-

from EasyLambda.Filesystem.BaseFilesystem import BaseFilesystem


class Stake:
    def __init__(
            self,
            filesystem,
            staking_strategy,
            staked_filename,
            staked_filename_remote=None
    ):
        """
        This should not be called directly, this class is used to return the details of successfully staked files

        :type filesystem: BaseFilesystem
        :param filesystem: The filesystem that contains the staked file

        :type staking_strategy: str
        :param staking_strategy: The staking strategy that was used to stake this file

        :type staked_filename: str
        :param staked_filename: The path/filename of the local copy of the staked file

        :type staked_filename_remote: str or None
        :param staked_filename_remote: The path/filename of staked file on the filesystem (if it has been renamed as part of the strategy)
        """
        self.__filesystem__ = filesystem
        self.__staking_strategy__ = staking_strategy
        self.__staked_filename__ = staked_filename
        self.__staked_filename_remote__ = staked_filename_remote

    def get_filesystem(self) -> BaseFilesystem:
        """
        Return the iterator that generated this staked file

        :return: Filesystem
        """
        return self.__filesystem__

    def get_staking_strategy(self) -> str:
        """
        Return the staking strategy that was used

        :return: str
        """
        return self.__staking_strategy__

    def get_staked_filename(self) -> str:
        """
        Return the filename of the local copy

        :return: str
        """
        return self.__staked_filename__

    def get_remote_staked_filename(self) -> str:
        """
        Return the filename of the staked file on the filesystem (if it has been renamed as part of the strategy)

        :return: str
        """
        return self.__staked_filename_remote__ or self.__staked_filename__
