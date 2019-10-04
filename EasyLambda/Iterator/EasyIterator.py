#!/usr/bin/env python
# -*- coding: utf-8 -*-

from EasyLambda.EasyLog import EasyLog
from EasyLambda.Iterator.Source.Source import Source
from time import strftime


class EasyIterator(EasyLog):
    # Filesystem constants
    FILESYSTEM_SOURCES = 'SOURCE'
    FILESYSTEM_ERROR_DESTINATIONS = 'ERROR'
    FILESYSTEM_SUCCESS_DESTINATIONS = 'SUCCESS'

    # Error constants
    ERROR_UNKNOWN_SOURCE_FILESYSTEM = 'Unknown source filesystem encountered'

    def __init__(
            self,
            aws_event,
            aws_context,
            easy_aws
    ):
        """
        :type aws_event: dict
        :param aws_event: AWS Lambda uses this parameter to pass in event data to the handler

        :type aws_context: LambdaContext
        :param aws_context: AWS Lambda uses this parameter to provide runtime information to your handler

        :type easy_aws: EasyAws
        :param easy_aws: EasyAws object used by this class

        :return: None
        """
        # Initialize logging class
        EasyLog.__init__(
            self=self,
            aws_event=aws_event,
            aws_context=aws_context,
            easy_aws=easy_aws
        )

        # Store session manager
        self.__easy_aws__ = easy_aws

        # Setup storage for filesystems we will be using
        self.__filesystems__ = {
            EasyIterator.FILESYSTEM_SOURCES: [],
            EasyIterator.FILESYSTEM_ERROR_DESTINATIONS: [],
            EasyIterator.FILESYSTEM_SUCCESS_DESTINATIONS: []
        }

        # Store the time current iteration operation started
        self.__current_iteration_start_time__ = ''

        # Store the file currently being iterated
        self.__current_file__ = None

    def add_source(self, source):
        """
        Add multiple input sources

        :type source: Source
        :param source: Source filesystem

        :return: None
        """
        self.__filesystems__[EasyIterator.FILESYSTEM_SOURCES].append(source)

    def get_sources(self) -> list:
        """
        Retrieve current list of file sources

        :return: list
        """
        self.log_trace('Retrieving source locations...')
        return self.__filesystems__[EasyIterator.FILESYSTEM_SOURCES]

    def clear_sources(self):
        """
        Remove all existing file sources

        :return: None
        """
        self.log_trace('Clearing sources...')
        self.__filesystems__[EasyIterator.FILESYSTEM_SOURCES] = []

    def add_errors_destination(self, destination):
        """
        Add error destination

        :type destination: Destination
        :param destination: Destination filesystem where files will be moved on error

        :return: None
        """
        self.log_trace('Adding error destination...')
        self.__filesystems__[EasyIterator.FILESYSTEM_SUCCESS_DESTINATIONS].append(destination)

    def get_errors_destinations(self) -> list:
        """
        Retrieve current list of error destinations

        :return: list
        """
        self.log_trace('Retrieving error destinations...')
        return self.__filesystems__[EasyIterator.FILESYSTEM_ERROR_DESTINATIONS]

    def clear_error_destinations(self):
        """
        Remove all existing error destinations

        :return: None
        """
        self.log_trace('Clearing error destinations...')
        self.__filesystems__[EasyIterator.FILESYSTEM_ERROR_DESTINATIONS] = []

    def add_success_destination(self, destination):
        """
        Add success destination

        :type destination: Destination
        :param destination: Destination filesystem where files will be moved on success

        :return: None
        """
        self.log_trace('Adding success destination...')
        self.__filesystems__[EasyIterator.FILESYSTEM_SUCCESS_DESTINATIONS].append(destination)

    def get_success_destinations(self) -> list:
        """
        Retrieve current list of success_destinations

        :return: list
        """
        self.log_trace('Retrieving success destinations...')
        return self.__filesystems__[EasyIterator.FILESYSTEM_SUCCESS_DESTINATIONS]

    def clear_success_destinations(self):
        """
        Remove all existing success destinations

        :return: None
        """
        self.log_trace('Clearing success destinations...')
        self.__filesystems__[EasyIterator.FILESYSTEM_SUCCESS_DESTINATIONS] = []

    def iterate_files(
            self,
            callback,
            maximum_files=None
    ):
        """
        Iterate all file sources

        :type callback: function
        :param callback: User callback function that is executed for each iterated file

        :type maximum_files: int
        :param maximum_files: The maximum number of files to iterate (across all sources)

        :return: list List of files that were iterated over
        """
        # Store the start time of the current iteration
        self.__current_iteration_start_time__ = strftime("%Y-%m-%d %H:%M:%S")

        # Maintain a list of files that have been iterated
        iterated_files = []

        # Go through each of the source filesystems
        for source in self.__filesystems__[EasyIterator.FILESYSTEM_SOURCES]:
            # Make sure it is the expected object type
            if isinstance(source, Source) is False:
                # This should never happen if the classes are used correctly
                raise Exception(EasyIterator.ERROR_UNKNOWN_SOURCE_FILESYSTEM)

            # Work out out many files we have left
            if maximum_files is None:
                remaining_files = None
            else:
                remaining_files = maximum_files - len(iterated_files)

            # Iterate the current file source, and append the iterated files to the list
            iterated_files.append(source.iterate_files(
                callback=callback,
                maximum_files=remaining_files,
                filesystems=self.__filesystems__
            ))

        # Return list of the iterated file objects
        return iterated_files
