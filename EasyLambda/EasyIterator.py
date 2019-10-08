#!/usr/bin/env python
# -*- coding: utf-8 -*-

from EasyLambda.Iterator.Destination import Destination
from EasyLambda.Iterator.Source import Source
from EasyLambda.EasyLog import EasyLog


class EasyIterator:
    # Error constants
    ERROR_NO_SOURCES_DEFINED = 'No iteration sources have been defined'
    ERROR_INVALID_ITERATOR_SOURCE = 'Unknown source type, expecting an iterator source of type EasyIteratorSource'
    ERROR_INVALID_ITERATOR_DESTINATION = 'Unknown destination type, expecting an iterator destination of type EasyIteratorDestination'

    def __init__(self):
        """
        Setup iterator
        """
        # Create storage for the relevant filesystems
        self.__sources__ = []
        self.__success_destinations__ = []
        self.__failure_destinations__ = []

    def get_sources(self) -> list:
        """
        Return list of source filesystem

        :return: list[EasyIteratorSource]
        """
        return self.__sources__

    def get_success_destinations(self) -> list:
        """
        Return list of success destinations

        :return: list[EasyIteratorDestination]
        """
        return self.__success_destinations__

    def get_failure_destinations(self) -> list:
        """
        Return list of failure destinations

        :return: list[EasyIteratorDestination]
        """
        return self.__failure_destinations__

    def add_source(self, source) -> None:
        """
        Add source filesystem

        :type source: Source
        :param source: Source filesystem to add

        :return: None  
        """
        EasyLog.trace('Adding source filesystem...')

        if isinstance(source, Source) is False:
            raise Exception(EasyIterator.ERROR_INVALID_ITERATOR_SOURCE)

        self.__sources__.append(source)

    def add_success_destination(self, destination) -> None:
        """
        Add destination filesystem for successfully iterated files

        :type destination: Destination
        :param destination: Destination filesystem to add

        :return: None  
        """
        EasyLog.trace('Adding success destination filesystem...')

        if isinstance(destination, Destination) is False:
            raise Exception(EasyIterator.ERROR_INVALID_ITERATOR_DESTINATION)

        self.__success_destinations__.append(destination)

    def add_failure_destination(self, destination) -> None:
        """
        Add destination filesystem for files that fail during iteration

        :type destination: Destination
        :param destination: Destination filesystem to add

        :return: None
        """
        EasyLog.trace('Adding failure destination filesystem...')

        if isinstance(destination, Destination) is False:
            raise Exception('Unknown destination type added, expecting an EasyIteratorDestination type')

        self.__failure_destinations__.append(destination)

    def iterate_sources(self, callback, maximum_files=None) -> list:
        """
        Iterate over all files in the sources

        :type callback: callable
        :param callback: Callable function that is executed as each file is retrieved

        :type maximum_files: int or None
        :param maximum_files: The maximum number of files to iterate

        :return: list[EasyIteratorStakedFile]
        """
        EasyLog.trace('Iterating all available file sources...')

        if len(self.__sources__) == 0:
            raise Exception(EasyIterator.ERROR_NO_SOURCES_DEFINED)

        staked_files = []

        source: Source
        for source in self.__sources__:
            EasyLog.debug('Iterating next file source...')

            # Sanity check the source type before we try to iterate
            if isinstance(source, Source) is False:
                raise Exception(EasyIterator.ERROR_INVALID_ITERATOR_SOURCE)

            # Work out how many files we can iterate
            if maximum_files is not None:
                remaining_files = maximum_files - len(staked_files)
            else:
                remaining_files = None

            # Iterate files in the current source
            staked_files.extend(source.iterate_files(
                callback=callback,
                maximum_files=remaining_files
            ))

        return staked_files
