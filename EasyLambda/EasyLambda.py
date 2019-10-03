#!/usr/bin/env python
# -*- coding: utf-8 -*-

from abc import abstractmethod

from EasyLambda.EasyLambdaSession import EasyLambdaSession
from EasyLambda.EasyLambdaCloudWatch import EasyLambdaCloudWatch
from EasyLambda.EasyLambdaLog import EasyLambdaLog


class EasyLambda(EasyLambdaSession, EasyLambdaCloudWatch, EasyLambdaLog):
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

        # Initialize AWS CloudWatch client
        EasyLambdaCloudWatch.__init__(
            self=self,
            aws_event=aws_event,
            aws_context=aws_context,
            easy_session_manager=self.get_easy_session_manager()
        )

        # Initialize logging class
        EasyLambdaLog.__init__(
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

    def validate_required_parameters(self, parameters_required):
        """
        Validate all required parameters were passed to the function

        :type parameters_required: tuple
        :param parameters_required: List of required parameters

        :return: None
        """
        self.log_debug('Validating required parameters...')
        parameter_error = False

        # Iterate all expected parameters
        for parameter in parameters_required:
            self.log_trace('Validating: {parameter}'.format(parameter=parameter))
            if self.get_aws_event_parameter(parameter=parameter) is None:
                # Required parameter was not found
                self.log_error('Missing parameter value: {parameter}'.format(parameter=parameter))
                self.put_cloudwatch_count(metric_name='parameter-validation-error')
                parameter_error = True

        # If any required parameter was missing, exit with a fatal error
        if parameter_error is True:
            self.put_cloudwatch_count('unhandled_exceptions')
            raise Exception('One or more required function parameter values was not supplied')

    @abstractmethod
    def run(self):
        """
        This function should be overridden with code to be executed

        :return: None
        """
        pass
