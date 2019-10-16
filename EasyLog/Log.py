#!/usr/bin/env python
# -*- coding: utf-8 -*-

import inspect
import os

from EasySlack.Client import Client as Slack
from time import strftime


# noinspection DuplicatedCode
class Log:
    # Logging level constants
    LEVEL_EXCEPTION = -2
    LEVEL_TEST = -1
    LEVEL_ERROR = 0
    LEVEL_INFO = 1
    LEVEL_WARNING = 2
    LEVEL_DEBUG = 3
    LEVEL_TRACE = 4

    # Private class variables
    __history__ = []
    __level__ = None

    __slack__ = None
    __slack_channel__ = None

    @staticmethod
    def set_level(level) -> None:
        """
        Set logging display level

        :type level: int
        :param level: Logging level, one of the LEVEL class constants

        :return: None
        """
        if level not in (Log.LEVEL_EXCEPTION, Log.LEVEL_TEST, Log.LEVEL_ERROR, Log.LEVEL_INFO, Log.LEVEL_WARNING, Log.LEVEL_DEBUG, Log.LEVEL_TRACE):
            raise Exception('Unknown logging level specified')

        Log.__level__ = level

    @staticmethod
    def slack_message(message) -> None:
        """
        Sent Slack message if it has been configured

        :type message: str
        :param message: The message to send
        """
        if Log.__slack__ is not None and Log.__slack_channel__ is not None:
            Log.__slack__.send_message(channel=Log.__slack_channel__, message=message)

    @staticmethod
    def slack_file(local_filename) -> None:
        """
        Sent file to Slack if it has been configured

        :type local_filename: str
        :param local_filename: The path/filename to send
        """
        if Log.__slack__ is not None and Log.__slack_channel__ is not None:
            Log.__slack__.send_file(channel=Log.__slack_channel__, local_filename=local_filename)

    @staticmethod
    def info(message) -> None:
        """
        Info level logging function

        :type message: str or Exception
        :param message: The message to be logged

        :return: None
        """
        # This has to be the first line in the function otherwise this will return the wrong stack frame
        stack_frame = inspect.stack()[1]

        Log.log(
            level=Log.LEVEL_INFO,
            stack_frame=stack_frame,
            message=message
        )

    @staticmethod
    def error(message) -> None:
        """
        Error level logging function

        :param message: Message to print
        :type message: str/Exception

        :return: None
        """
        # This has to be the first line in the function otherwise this will return the wrong stack frame
        stack_frame = inspect.stack()[1]

        Log.log(
            level=Log.LEVEL_ERROR,
            stack_frame=stack_frame,
            message=message
        )

        if Log.__slack__ is not None and Log.__slack_channel__ is not None:
            # Send error messages to designated slack channel
            message_formatted = Log.format_message(level=Log.LEVEL_ERROR, message=message, stack_frame=stack_frame)
            Log.__slack__.send_message(
                channel=Log.__slack_channel__,
                message=message_formatted
            )

    @staticmethod
    def warning(message) -> None:
        """
        Warning level logging function

        :param message: Message to print
        :type message: str/Exception

        :return: None
        """
        # This has to be the first line in the function otherwise this will return the wrong stack frame
        stack_frame = inspect.stack()[1]

        Log.log(
            level=Log.LEVEL_WARNING,
            stack_frame=stack_frame,
            message=message
        )

        if Log.__slack__ is not None and Log.__slack_channel__ is not None:
            # Send error messages to designated slack channel
            message_formatted = Log.format_message(level=Log.LEVEL_WARNING, message=message, stack_frame=stack_frame)
            Log.__slack__.send_message(
                channel=Log.__slack_channel__,
                message=message_formatted
            )

    @staticmethod
    def debug(message) -> None:
        """
        Debug level logging function

        :param message: Message to print
        :type message: str/Exception

        :return: None
        """
        # This has to be the first line in the function otherwise this will return the wrong stack frame
        stack_frame = inspect.stack()[1]

        Log.log(
            level=Log.LEVEL_DEBUG,
            stack_frame=stack_frame,
            message=message
        )

    @staticmethod
    def test(message) -> None:
        """
        Unit test level logging function

        :param message: Message to print
        :type message: str/Exception

        :return: None
        """
        # This has to be the first line in the function otherwise this will return the wrong stack frame
        stack_frame = inspect.stack()[1]

        Log.log(
            level=Log.LEVEL_TEST,
            stack_frame=stack_frame,
            message=message
        )

    @staticmethod
    def trace(message) -> None:
        """
        Trace level logging function

        :param message: Message to print
        :type message: str/Exception

        :return: None
        """
        # This has to be the first line in the function otherwise this will return the wrong stack frame
        stack_frame = inspect.stack()[1]

        Log.log(
            level=Log.LEVEL_TRACE,
            stack_frame=stack_frame,
            message=message
        )

    @staticmethod
    def exception(message, base_exception=None) -> None:
        """
        Exception error logging function

        :type message: str
        :param message: Message to print

        :type base_exception: Exception or str or None
        :param base_exception: The exception error that was raised

        :return: None
        """
        # This has to be the first line in the function otherwise this will return the wrong stack frame
        stack_frame = inspect.stack()[1]

        Log.log(level=Log.LEVEL_EXCEPTION, stack_frame=stack_frame, message=message)

        # If no base base_exception was sent through, throw the message as the base_exception
        if base_exception is None:
            raise Exception(message)

        Log.log(level=Log.LEVEL_EXCEPTION, message=base_exception)

        if Log.__slack__ is not None and Log.__slack_channel__ is not None:
            # Send error messages to designated slack channel
            message_formatted = Log.format_message(level=Log.LEVEL_EXCEPTION, message=message, stack_frame=stack_frame)
            Log.__slack__.send_message(
                channel=Log.__slack_channel__,
                message=message_formatted
            )
        # Raise the base exception
        raise Exception(str(base_exception))

    @staticmethod
    def get_log_level_name(level) -> str:
        """
        Retrieve the name fo the logging level

        :type level: int
        :param level: The levels integer ID

        :return: The name of the logging level
        """
        if level == Log.LEVEL_TEST:
            level_name = 'TEST'
        elif level == Log.LEVEL_ERROR:
            level_name = 'ERROR'
        elif level == Log.LEVEL_EXCEPTION:
            level_name = 'EXCEPTION'
        elif level == Log.LEVEL_INFO:
            level_name = 'INFO'
        elif level == Log.LEVEL_WARNING:
            level_name = 'WARNING'
        elif level == Log.LEVEL_DEBUG:
            level_name = 'DEBUG'
        elif level == Log.LEVEL_TRACE:
            level_name = 'TRACE'
        else:
            raise Exception('Unknown logging level specified')

        return level_name

    @staticmethod
    def log(level, message, stack_frame=None) -> None:
        # If no logging level is defined, select one based on the current context
        if Log.__level__ is None:
            # Work out if we are in a unit test
            current_stack = inspect.stack()
            is_unit_test = False
            for stack_frame in current_stack:
                for program_line in stack_frame[4]:
                    if "unittest" in program_line:
                        is_unit_test = True
                        break

            if is_unit_test is True:
                # Running unit tests, disable logging
                print('Running unit tests, logging test messages only...')
                Log.__level__ = Log.LEVEL_TEST
            else:
                # Not running unit tests, default to maximum logging level
                print('No logging level has been defined, defaulting to maximum logging...')
                Log.__level__ = Log.LEVEL_TRACE

        # Convert the log level to a human readable string
        level_name = Log.get_log_level_name(level)

        # Retrieve current timestamp
        timestamp = strftime("%Y-%m-%d %H:%M:%S")

        # Trim whitespace off the message
        message = str(message).strip()

        # Create history entry
        history = {
            'message': message,
            'message_formatted': '',
            'level': level_name,
            'timestamp': timestamp,
            'filename': '',
            'function': '',
            'line_number': '',
        }

        # Create a display formatted version of the message
        message_formatted = message

        # If we received a stack frame, add its details to the log entry
        if stack_frame is not None:
            history['filename'] = stack_frame.filename
            history['function'] = stack_frame.function
            history['line_number'] = stack_frame.lineno
            message_formatted = '[{level_name}: {filename}] {function}():{line_number} - {message_formatted}'.format(
                level_name=level_name,
                filename=os.path.basename(history['filename']),
                function=history['function'],
                line_number=history['line_number'],
                message_formatted=message_formatted
            )

        message_formatted = '[{timestamp}] {message_formatted}'.format(timestamp=timestamp, message_formatted=message_formatted)
        history['message_formatted'] = message_formatted

        # Display the message if appropriate based on the current log level
        if Log.__level__ is None or Log.__level__ >= level:
            print(message_formatted)

        # Add entry to the log
        Log.__history__.append(history)

    @staticmethod
    def format_message(level, message, stack_frame=None) -> str:
        # Create a display formatted version of the message
        message_formatted = message
        timestamp = strftime("%Y-%m-%d %H:%M:%S")
        level_name = Log.get_log_level_name(level)

        # If we received a stack frame, add its details to the log entry
        if stack_frame is not None:
            message_formatted = '[{level_name}: {filename}] {function}():{line_number} - {message_formatted}'.format(
                level_name=level_name,
                filename=stack_frame.filename,
                function=stack_frame.function,
                line_number=stack_frame.lineno,
                message_formatted=message_formatted
            )

        return '[{timestamp}] {message_formatted}'.format(timestamp=timestamp, message_formatted=message_formatted)

    @staticmethod
    def set_slack_client(slack, channel=None) -> None:
        """
        Set Slack client for error/exception logging

        :type slack: Slack or None
        :param slack: The Slack client to use

        :type channel: Optional[str]
        :param channel: Then channel to send errors/exceptions to
        """
        if slack is not None:
            if isinstance(slack, Slack) is False:
                raise Exception('Invalid Slack client object supplied.')
            if channel is None:
                raise Exception('No Slack channel provided')
        else:
            channel = None

        Log.__slack__ = slack
        Log.__slack_channel__ = channel

    @staticmethod
    def clear_log_history() -> None:
        """
        Clear any existing log history

        :return: None
        """
        Log.__history__ = []

    @staticmethod
    def get_log_history(self) -> list:
        """
        Return the complete log history regardless of the current log level

        :return: list
        """
        return self.__history__
