#!/usr/bin/env python
# -*- coding: utf-8 -*-

from EasyLambda.Filesystem.BaseFilesystem import BaseFilesystem
from EasyLambda.Filesystem.Stake import Stake


class File:
    def __init__(self, filesystem, filename, stake=None):
        """
        :type filesystem: BaseFilesystem
        :param filesystem: The filesystem that contains this file

        :type filename: str
        :param filename: The path/filename to the file on the filesystem

        :type stake: Stake or None
        :param stake: The staking details if applicable
        """
        self.__filesystem__ = filesystem
        self.__filename__ = filename
        self.__stake__ = stake

    def get_filesystem(self) -> BaseFilesystem:
        """
        Return the underlying filesystem

        :return: str
        """
        return self.__filesystem__

    def get_filename(self) -> str:
        """
        Return the filename/path to the file in the underlying filesystem

        :return: str
        """
        return self.__filename__

    def is_staked(self) -> bool:
        """
        Check if there is staking information for this file

        :return: bool
        """
        return self.__stake__ is not None

    def get_stake(self) -> Stake:
        """
        Return staking information

        :return: Stake
        """
        return self.__stake__

    def set_stake(self, stake) -> None:
        """
        Set staking strategy for the file

        :type stake: Stake
        :param stake: The staking details

        :return:
        """
        self.__stake__ = stake
