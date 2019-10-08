#!/usr/bin/env python
# -*- coding: utf-8 -*-

from abc import abstractmethod
from EasyLambda.EasyCloudWatch import EasyCloudWatch
from EasyLambda.EasyLog import EasyLog


class EasyLambda:
    def __init__(self, aws_event, aws_context):
        """
        :type aws_event: dict
        :param aws_event: AWS Lambda uses this parameter to pass in event data to the handler

        :type aws_context: LambdaContext
        :param aws_context: AWS Lambda uses this parameter to provide runtime information to your handler
        """
        self.__aws_event__ = aws_event
        self.__aws_context__ = aws_context

        try:
            EasyLog.trace('Executing user initialization function...')
            self.init()
        except Exception as init_exception:
            # Something went wrong inside the users init function- log the error
            EasyLog.exception('Unhandled exception during execution of user initialization function', init_exception)
            raise init_exception

        try:
            EasyLog.trace('Executing user run function...')
            self.run()
        except Exception as run_exception:
            # Something went wrong inside the users run function- log the error
            EasyLog.error('Unhandled exception during execution of user run function:\n{run_exception}'.format(run_exception=run_exception))
            raise run_exception

        # Execution completed, log out the time remaining- this may be useful for tracking bloat/performance degradation over the life of the Lambda function
        time_remaining = self.get_aws_time_remaining()
        EasyLog.info('Execution completed with {time_remaining} milliseconds remaining'.format(time_remaining=time_remaining))
        EasyCloudWatch.put_metric('lambda_time_remaining', time_remaining, 'Milliseconds')

    @abstractmethod
    def init(self) -> None:
        """
        This function should be overridden with code to be executed prior to running the main run function

        :return: None
        """
        pass

    @abstractmethod
    def run(self) -> None:
        """
        This function should be overridden with the main application code

        :return: None
        """
        pass

    # AWS Information Retrieval

    def get_aws_context(self):
        """
        Return the AWS event context

        :return: LambdaContext
        """
        return self.__aws_context__

    def get_aws_event(self) -> dict:
        """
        Return the AWS event parameter

        :return: dict
        """
        return self.__aws_event__

    def get_aws_event_parameter(self, parameter):
        """
        Return the AWS event parameter, or None if does not exist

        :return: None or str
        """
        if parameter not in self.__aws_event__:
            return None

        return self.__aws_event__[parameter]

    def get_aws_request_id(self) -> str:
        """
        Get the unique AWS request ID

        :return: str
        """
        return self.__aws_context__.aws_request_id

    def get_aws_function_arn(self) -> str:
        """
        Return the ARN of the AWS function being executed

        :return: str
        """
        return self.__aws_context__.invoked_function_arn

    def get_aws_function_name(self) -> str:
        """
        Return the name of the AWS function being executed

        :return: str
        """
        return self.__aws_context__.function_name

    def get_aws_time_remaining(self) -> int:
        """
        Return the number of milliseconds remaining before the Lambda times out

        :return: int
        """
        return self.__aws_context__.get_remaining_time_in_millis()
