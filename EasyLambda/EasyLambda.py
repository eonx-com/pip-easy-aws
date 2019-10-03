#!/usr/bin/env python
# -*- coding: utf-8 -*-

from abc import abstractmethod

from EasyLambda.EasyCloudWatch import EasyCloudWatch
from EasyLambda.EasyIterator import EasyIterator
from EasyLambda.EasyLog import EasyLog
from EasyLambda.EasyLambdaSession import EasyLambdaSession
from EasyLambda.EasyValidator import EasyValidator


# noinspection PyMethodMayBeStatic
class EasyLambda(EasyLambdaSession, EasyValidator, EasyCloudWatch, EasyLog):
    def __init__(self, aws_event, aws_context):
        """
        :type aws_event: dict
        :param aws_event: AWS Lambda uses this parameter to pass in event data to the handler

        :type aws_context: LambdaContext
        :param aws_context: AWS Lambda uses this parameter to provide runtime information to your handler

        :return: None
        """
        # Initialize AWS Session Manager
        EasyLambdaSession.__init__(
            self=self,
            aws_event=aws_event,
            aws_context=aws_context
        )

        # Initialize parameter validation class
        EasyValidator.__init__(self=self)

        # Initialize AWS CloudWatch client
        EasyCloudWatch.__init__(
            self=self,
            aws_event=aws_event,
            aws_context=aws_context,
            easy_session_manager=self.get_easy_session_manager()
        )

        # Initialize logging class
        EasyLog.__init__(
            self=self,
            aws_event=aws_event,
            aws_context=aws_context,
            easy_session_manager=self.get_easy_session_manager()
        )

        try:
            self.log_trace('Executing user run function...')
            self.run()
        except Exception as run_exception:
            self.put_cloudwatch_count('unhandled_exceptions')
            self.log_error('Unhandled exception during execution user function: {run_exception}'.format(
                run_exception=run_exception
            ))

        # Display the time remaining on completion of user code
        time_remaining = self.get_aws_time_remaining()
        self.log_debug('Execution completed with {time_remaining} milliseconds remaining'.format(
            time_remaining=time_remaining
        ))

        # Add CloudWatch metric to monitor time remaining
        self.put_cloudwatch_custom_metric(
            metric_name='time_remaining',
            value=time_remaining,
            unit='Milliseconds'
        )

    def get_iterator(self) -> EasyIterator:
        """
        Return file iterator

        :return: EasyIterator
        """
        return EasyIterator(
            aws_event=self.__aws_event__,
            aws_context=self.__aws_context__,
            easy_session_manager=self.get_easy_session_manager()
        )

    @abstractmethod
    def run(self):
        """
        This function should be overridden with code to be executed

        :return: None
        """
        pass
