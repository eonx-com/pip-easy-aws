#!/usr/bin/env python
# -*- coding: utf-8 -*-

from EasyFilesystem.Iterator.Destination import Destination
from EasyFilesystem.Iterator.Source import Source
from EasyLog.Log import Log


class Iterator:
    # Error constants
    ERROR_INVALID_SOURCE_FILESYSTEM = 'An unknown filesystem type was specified when attempting to add a new source'
    ERROR_INVALID_DESTINATION_FILESYSTEM = 'An unknown filesystem type was specified when attempting to add a new destination'
    ERROR_NO_SOURCE_FILESYSTEM = 'No source filesystems have been added, iteration cannot proceed'

    def __init__(self):
        """
        Setup iterator
        """
        # Create storage for the relevant filesystems
        self.__sources__ = []
        self.__destinations_success__ = []
        self.__destinations_failure__ = []

    def get_destinations_failure(self) -> list:
        """
        Return list of failure destinations

        :return: list[EasyIteratorDestination]
        """
        return self.__destinations_failure__

    def get_destinations_success(self) -> list:
        """
        Return list of success destinations

        :return: list[EasyIteratorDestination]
        """
        return self.__destinations_success__

    def get_sources(self) -> list:
        """
        Return list of source filesystem

        :return: list[EasyIteratorSource]
        """
        return self.__sources__

    def add_destination_failure(self, destination) -> None:
        """
        Add destination filesystem for files that fail during iteration

        :type destination: Destination
        :param destination: Destination filesystem to add

        :return: None
        """
        Log.trace('Adding failure destination filesystem...')

        if isinstance(destination, Destination) is False:
            raise Exception('Unknown destination type added, expecting an EasyIteratorDestination type')

        self.__destinations_failure__.append(destination)

    def add_destination_success(self, destination) -> None:
        """
        Add destination filesystem for successfully iterated files

        :type destination: Destination
        :param destination: Destination filesystem to add

        :return: None
        """
        Log.trace('Adding success destination filesystem...')

        if isinstance(destination, Destination) is False:
            raise Exception(Iterator.ERROR_INVALID_DESTINATION_FILESYSTEM)

        self.__destinations_success__.append(destination)

    def add_source(self, source) -> None:
        """
        Add source filesystem

        :type source: Source
        :param source: Source filesystem to add

        :return: None
        """
        Log.trace('Adding source filesystem...')

        if isinstance(source, Source) is False:
            raise Exception(Iterator.ERROR_INVALID_SOURCE_FILESYSTEM)

        self.__sources__.append(source)

    def iterate_sources(self, callback, maximum_files=None) -> list:
        """
        Iterate over all files in the sources

        :type callback: callable
        :param callback: Callable function that is executed as each file is retrieved

        :type maximum_files: int or None
        :param maximum_files: The maximum number of files to iterate

        :return: list[EasyIteratorStakedFile]
        """
        Log.trace('Iterating source filesystems...')

        # Retrieve list of sources
        sources = self.get_sources()

        # If no sources have been defined raise an base_exception error
        count_sources = len(sources)

        if count_sources == 0:
            Log.error(Iterator.ERROR_NO_SOURCE_FILESYSTEM)
            raise Exception(Iterator.ERROR_NO_SOURCE_FILESYSTEM)

        # Maintain a list of all iterated files
        iterated_files = []

        count_current_source = 1

        source: Source
        for source in sources:
            Log.debug('Iterating source {count_current_source}/{count_sources}...'.format(
                count_current_source=count_current_source,
                count_sources=count_sources
            ))

            # Work out how many files we have been allowed to iterate, and how many we have left if this is not the first source...
            if maximum_files is not None:
                remaining_files = maximum_files - len(iterated_files)
            else:
                remaining_files = None

            # Iterate files in the current source and append the resulting files to our list for later return
            iterated_files.extend(
                source.iterate_files(
                    callback=callback,
                    maximum_files=remaining_files
                )
            )

            # Keep track of which source we are currently iterating
            count_current_source += 1

        # Return list of all files that we iterated
        return iterated_files
