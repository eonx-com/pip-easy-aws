#!/usr/bin/env python
# -*- coding: utf-8 -*-

import traceback


class EasyLambdaLog(object):
    def __init__(self, aws_event, aws_context, easy_session_manager):
        """
        :type aws_event: dict
        :param aws_event: AWS Lambda uses this parameter to pass in event data to the handler

        :type aws_context: LambdaContext
        :param aws_context: AWS Lambda uses this parameter to provide runtime information to your handler

        :return: None
        """
        self.__aws_context__ = aws_context
        self.__aws_event__ = aws_event
        self.__log_level__ = 3
        self.__log_history__ = ''

        # Default the namespace to the function name
        self.set_log_namespace(namespace=self.__aws_context__.function_name)

        # Get CloudWatch client so we can record metrics on errors/warnings
        self.__easy_cloudwatch_client__ = easy_session_manager.get_cloudwatch_client()

    # Getter/setter for logging namespace

    def get_log_namespace(self) -> str:
        """
        Get the namespace for log messages

        :return: str
        """
        return self.__log_namespace__ or ''

    def set_log_namespace(self, namespace) -> None:
        """
        Set the namespace to display in log messages
        
        :type namespace: str
        :param namespace: The namespace to display in logs
        
        :return: None
        """
        namespace = str(namespace).upper()
        self.__log_namespace__ = '[{namespace}] '.format(namespace=namespace)

    # Getter/setter for logging level

    def get_log_level(self) -> int:
        """
        Get the current log level

        :return: int
        """
        return self.__log_level__

    def set_log_level(self, log_level=1) -> None:
        """
        Enable debug logging

        :type log_level: int
        :param log_level: Logging level

        :return: None
        """
        if self.__log_level__ == 0:
            level = 'Standard'
        elif self.__log_level__ == 1:
            level = 'Warning'
        elif self.__log_level__ == 2:
            level = 'Debug'
        elif self.__log_level__ >= 3:
            level = 'Trace'
        else:
            raise Exception('Unknown logging level specified')

        self.__log_level__ = log_level
        print('{level} level logging enabled'.format(level=level))

    # Logging Functions

    def log(self, message) -> None:
        """
        Print standard log message

        :param message: Message to print
        :type message: str/Exception

        :return: None
        """
        message = '{namespace}{message}'.format(
            message=message,
            namespace=self.get_log_namespace()
        )

        self.__log_history_append__(message)

        # Output log message
        print(message)

    def log_error(self, message) -> None:
        """
        Print error log message

        :param message: Message to print
        :type message: str/Exception

        :return: None
        """
        self.__easy_cloudwatch_client__.put_cloudwatch_count('errors')

        message = 'ERROR {namespace}{message}'.format(message=message, namespace=self.get_log_namespace())
        self.__log_history_append__(message)

        print(message)

    def log_warning(self, message) -> None:
        """
        Print warning log message

        :param message: Message to print
        :type message: str/Exception

        :return: None
        """
        self.__easy_cloudwatch_client__.put_cloudwatch_count('warnings')

        message = 'WARNING {namespace}{message}'.format(message=message, namespace=self.get_log_namespace())
        self.__log_history_append__(message)

        if self.__log_level__ >= 1:
            print(message)

    def log_debug(self, message) -> None:
        """
        Print debugging log message only if the global value 'debug_logging' is set to True

        :param message: Message to print
        :type message: str/Exception

        :return: None
        """
        message = 'DEBUG {namespace}{message}'.format(message=message, namespace=self.get_log_namespace())
        self.__log_history_append__(message)

        if self.__log_level__ >= 2:
            print(message)

    def log_trace(self, message) -> None:
        """
        Print debugging log message only if the global value 'debug_logging' is set to True

        :param message: Message to print
        :type message: str/Exception

        :return: None
        """
        message = 'TRACE {namespace}{message}'.format(message=message, namespace=self.get_log_namespace())
        self.__log_history_append__(message)

        if self.__log_level__ >= 3:
            print(message)

    def log_stack_trace(self) -> None:
        """
        Add stack trace to the log

        :return: None
        """
        for line in traceback.format_stack():
            self.log(line.strip())

    # Logging history functions

    def clear_log_history(self) -> None:
        """
        Clear any existing log history

        :return: None
        """
        self.__log_history__ = ''

    def get_log_history(self) -> str:
        """
        Return the complete log history regardless of the current log level

        :return: str
        """
        return self.__log_history__

    # Internal function

    def __log_history_append__(self, message) -> None:
        """
        Append the message to the log history

        :type message: str
        :param message: Message to log into the history

        :return: None
        """
        self.__log_history__ = "{log_history}\n{message}\n".format(
            log_history=self.__log_history__,
            message=str(message).strip()
        )
