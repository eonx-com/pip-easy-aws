#!/usr/bin/env python
# -*- coding: utf-8 -*-

import inspect

from time import strftime
from EasyLambda.EasyCloudWatch import EasyCloudWatch


class EasyLog:
    # Logging level constants
    LEVEL_ERROR = 0
    LEVEL_INFO = 1
    LEVEL_WARNING = 2
    LEVEL_DEBUG = 3
    LEVEL_TRACE = 4

    # Private class variables
    __history__ = []
    __level__ = None
    __cloudwatch_client__ = None

    @staticmethod
    def set_level(level) -> None:
        """
        Set logging display level

        :type level: int
        :param level: Logging level, one of the LEVEL class constants

        :return: None
        """
        if level not in (EasyLog.LEVEL_INFO, EasyLog.LEVEL_WARNING, EasyLog.LEVEL_DEBUG, EasyLog.LEVEL_TRACE):
            raise Exception('Unknown logging level specified')

        EasyLog.__level__ = level

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

        EasyLog.log(
            level=EasyLog.LEVEL_INFO,
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

        EasyLog.log(
            level=EasyLog.LEVEL_ERROR,
            stack_frame=stack_frame,
            message=message
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

        EasyLog.log(
            level=EasyLog.LEVEL_WARNING,
            stack_frame=stack_frame,
            message=message
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

        EasyLog.log(
            level=EasyLog.LEVEL_DEBUG,
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

        EasyLog.log(
            level=EasyLog.LEVEL_TRACE,
            stack_frame=stack_frame,
            message=message
        )

    @staticmethod
    def exception(message, exception) -> None:
        """
        Exception error logging function

        :type message: str
        :param message: Message to print

        :type exception: Exception
        :param exception: The exception error that was raised

        :return: None
        """
        # This has to be the first line in the function otherwise this will return the wrong stack frame
        stack_frame = inspect.stack()[1]

        EasyLog.log(level=EasyLog.LEVEL_ERROR, stack_frame=stack_frame, message=message)
        EasyLog.log(level=EasyLog.LEVEL_ERROR, message=exception)

        EasyCloudWatch.increment_count('count_exceptions')

    @staticmethod
    def log(level, message, stack_frame=None) -> None:
        # Convert the log level to a human readable string
        if level == EasyLog.LEVEL_ERROR:
            level_name = 'ERROR'
            if EasyLog.__cloudwatch_client__ is not None:
                EasyCloudWatch.increment_count('count_errors')
                pass
        elif level == EasyLog.LEVEL_INFO:
            level_name = 'INFO'
        elif level == EasyLog.LEVEL_WARNING:
            level_name = 'WARNING'
            if EasyLog.__cloudwatch_client__ is not None:
                EasyCloudWatch.increment_count('count_warnings')
                pass
        elif level == EasyLog.LEVEL_DEBUG:
            level_name = 'DEBUG'
        elif level == EasyLog.LEVEL_TRACE:
            level_name = 'TRACE'
        else:
            raise Exception('Unknown logging level specified')

        # Retrieve current timestamp
        timestamp = strftime("%Y-%m-%d %H:%M:%S")

        # Trim whitespace off the message
        message = message.strip()

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
            history.filename = stack_frame.filename
            history.function = stack_frame.function
            history.line_number = stack_frame.lineno
            message_formatted = '{level_name} {filename}:{function} ({line_number}) - {message_formatted}'.format(
                level_name=level_name,
                filename=history.filename,
                function=history.function,
                line_number=history.line_number,
                message_formatted=message_formatted
            )

        message_formatted = '[{timestamp}] {message_formatted}'.format(timestamp=timestamp, message_formatted=message_formatted)
        history.message_formatted = message_formatted

        # Display the message if appropriate based on the current log level
        if EasyLog.__level__ >= level:
            print(message_formatted)

        # Add entry to the log
        EasyLog.__history__.append(history)

    @staticmethod
    def clear_log_history() -> None:
        """
        Clear any existing log history

        :return: None
        """
        EasyLog.__history__ = []

    @staticmethod
    def get_log_history(self) -> list:
        """
        Return the complete log history regardless of the current log level

        :return: list
        """
        return self.__history__
