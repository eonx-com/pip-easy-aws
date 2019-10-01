#!/usr/bin/env python
# -*- coding: utf-8 -*-

import traceback

from abc import abstractmethod
from EasyBoto3.EasyCloudWatch import EasyCloudWatch
from EasyBoto3.EasySessionManager import EasySessionManager


# noinspection PyMethodMayBeStatic
class EasyLambda:
    def __init__(
            self,
            aws_event,
            aws_context,
            region=None,
            credentials=None
    ):
        """
        Initialize Lambda function

        :type aws_event: dict
        :param aws_event: AWS Lambda uses this parameter to pass in event data to the handler

        :type aws_context: LambdaContext
        :param aws_context: AWS Lambda uses this parameter to provide runtime information to your handler

        :type region: string/None
        :param region: The AWS region in which the session should be opened. If not specified the current region
            reported by Boto3 will be used

        :type credentials: dict/None
        :param credentials: Optional dictionary containing 'aws_access_key_id' and 'aws_secret_access_key' to use
            when connecting to AWS. If not supplied the session will be established using the credentials available
            on the host machine. It is recommended to leave this blank and use IAM roles to provide access to required
            resources

        :return: None
        """
        # Store the AWS event and context
        self.aws_context = aws_context
        self.aws_event = aws_event

        # Set logging level
        self.log_level = 3
        if 'log_level' in self.aws_event:
            self.set_log_level(log_level=int(self.aws_event['log_level']))
        else:
            self.log('No logging level was defined in function configuration, defaulting to maximum debug logging')

        # Store user values in a dictionary- this means they can be dumped on a fatal error to aid with debugging
        self.user_data = {}

        # Ensure the function stage parameter was supplied
        self.stage = self.get_aws_event_parameter('stage')
        if self.stage is None:
            self.exit_fatal_error('Function was called without required "stage" parameter')

        # Get AWS session manager
        self.log_trace('Retrieving EasySessionManager object...')
        self.easy_session_manager = EasySessionManager(
            region=region,
            credentials=credentials,
            log_level=self.log_level
        )

        # Get CloudWatch client
        self.log_trace('Retrieving EasyCloudWatch client...')
        self.cloudwatch_client = self.easy_session_manager.get_cloudwatch_client()

        try:
            # Execute the user function code
            self.log_trace('Executing user run function...')
            self.run()
        except Exception as run_exception:
            # On any unhandled exception generated a fatal error
            self.exit_fatal_error('Unhandled exception during execution of Lambda function: {run_exception}'.format(
                run_exception=run_exception
            ))

    @abstractmethod
    def run(self):
        """
        This function should be overridden with code to be executed
        :return: None
        """
        pass

    # Parameter Validation

    def validate_required_parameters(self, parameters_required):
        """
        Validate all required parameters were passed to the function

        :type parameters_required: tuple
        :param parameters_required: List of required parameters

        :return: None
        """
        self.log_trace('Validating required parameters...')
        parameter_error = False

        # Iterate all expected parameters
        for parameter in parameters_required:
            self.log_trace('Checking required parameter: {parameter}'.format(parameter=parameter))
            if self.get_aws_event_parameter(parameter=parameter) is None:
                # Required parameter was not found
                self.log_error('Missing parameter value: {parameter}'.format(parameter=parameter))
                self.put_cloudwatch_count(metric_name='parameter-validation-error')
                parameter_error = True
            else:
                # Display debugging message with value
                self.log_trace('Found value: {value}'.format(value=self.get_aws_event_parameter(parameter=parameter)))

        # If any required parameter was missing, exit with a fatal error
        if parameter_error is True:
            self.exit_fatal_error('One or more required function parameter values was not supplied')

    # User Data Handling

    def set_value(self, name, value):
        """
        Set a user value

        :type name: str
        :param name: Name of value to set

        :param value: The value to set

        :return: None
        """
        self.log_trace('Setting user value: {name}={value}'.format(name=name, value=value))
        self.user_data[name] = value

    def get_value(self, name):
        """
        Return a user value

        :type name: str
        :param name: Name of value to retrieve

        :return:
        """
        self.log_trace('Getting user value: {name}'.format(name=name))

        if name not in self.user_data:
            self.log_trace('User value not set, returning None...')
            return None

        self.log_trace('User value found: {value}'.format(value=self.user_data[name]))
        return self.user_data[name]

    # AWS Information Retrieval

    def get_stage(self) -> str:
        """
        Return the deployment stage this function belongs to

        :return: str
        """
        self.log_trace('Retrieving stage...')
        return self.stage

    def get_aws_event(self):
        """
        Return the AWS event variable

        :return: dict
        """
        self.log_trace('Retrieving AWS event...')
        return self.aws_event

    def get_aws_event_parameter(self, parameter):
        """
        Return the AWS event parameter, or None if does not exist

        :return: dict
        """
        aws_event = self.get_aws_event()
        self.log_trace('Retrieving parameter: {parameter}...'.format(parameter=parameter))

        if parameter not in aws_event:
            self.log_trace('Parameter did not exist')
            return None

        self.log_trace('Found parameter value: {value}...'.format(value=aws_event[parameter]))
        return aws_event[parameter]

    def get_aws_context(self):
        """
        Return the AWS context variable

        :return: LambdaContext
        """
        return self.aws_context

    def get_aws_session_manager(self) -> EasySessionManager:
        """
        Return the AWS session manager

        :return: EasySessionManager
        """
        self.log_trace('Retrieving AWS session manager...')
        return self.easy_session_manager

    def get_aws_cloudwatch_client(self) -> EasyCloudWatch:
        self.log_trace('Retrieving AWS CloudWatch client...')
        return self.cloudwatch_client

    def get_aws_request_id(self) -> str:
        """
        Get the unique AWS request ID
        :return: str
        """
        self.log_trace('Retrieving AWS request ID...')
        return self.get_aws_context().aws_request_id

    def get_aws_function_arn(self) -> str:
        """
        Return the ARN of the AWS function being executed

        :return: str
        """
        self.log_trace('Retrieving AWS function ARN...')
        return self.get_aws_context().invoked_function_arn

    def get_aws_function_name(self) -> str:
        """
        Return the name of the AWS function being executed

        :return: str
        """
        return self.get_aws_context().function_name

    def def_aws_time_remaining(self) -> int:
        """
        Return the number of milliseconds remaining before the Lambda times out

        :return: int
        """
        self.log_trace('Retrieving AWS execution time remaining...')
        return self.get_aws_context().get_remaining_time_in_millis()

    # Logging Level Functions

    def set_log_level(self, log_level=1):
        """
        Enable debug logging

        :type log_level: int
        :param log_level: Logging level

        :return: None
        """
        self.log_level = log_level

        if self.log_level == 0:
            print('Standard logging level')
        elif self.log_level == 1:
            print('Warning logging enabled')
        elif self.log_level == 2:
            print('Debug logging enabled')
        elif self.log_level >= 3:
            print('Trace logging enabled')

    # Logging Functions

    def log(self, message):
        """
        Print standard log message

        :param message: Message to print
        :type message: str/Exception

        :return: None
        """
        print('[{function_name}] {message}'.format(
            message=message,
            function_name=self.get_aws_function_name()
        ))

    def log_error(self, message):
        """
        Print error log message

        :param message: Message to print
        :type message: str/Exception

        :return: None
        """
        self.put_cloudwatch_count(metric_name='error')

        print('[{function_name}] ERROR {message}'.format(
            message=message,
            function_name=self.get_aws_function_name()
        ))

    def log_warning(self, message):
        """
        Print warning log message

        :param message: Message to print
        :type message: str/Exception

        :return: None
        """
        self.put_cloudwatch_count(metric_name='warning')

        if self.log_level >= 1:
            print('WARNING [{function_name}] {message}'.format(
                message=message,
                function_name=self.get_aws_function_name()
            ))

    def log_debug(self, message):
        """
        Print debugging log message only if the global value 'debug_logging' is set to True

        :param message: Message to print
        :type message: str/Exception

        :return: None
        """
        if self.log_level >= 2:
            print('DEBUG [{function_name}] {message}'.format(
                message=message,
                function_name=self.get_aws_function_name()
            ))

    def log_trace(self, message):
        """
        Print debugging log message only if the global value 'debug_logging' is set to True

        :param message: Message to print
        :type message: str/Exception

        :return: None
        """
        if self.log_level >= 3:
            print('TRACE [{function_name}] {message}'.format(
                message=message,
                function_name=self.get_aws_function_name()
            ))

    def exit_fatal_error(self, message):
        """
        Terminate execution after logging a fatal error message, reports error code 911 and adds a CloudWatch
        fatal error metric count

        :return: None
        """
        # Add CloudWatch metric
        self.put_cloudwatch_count(metric_name='fatal-error')

        # Log error
        print('FATAL ERROR [{function_name}] {message}'.format(
            message=message,
            function_name=self.get_aws_function_name()
        ))

        # Print stack trace
        for line in traceback.format_stack():
            print(line.strip())

        # Terminator process with 911 error code
        exit(911)

    # CloudWatch Metrics

    def put_cloudwatch_custom_metric(self, metric_name, value, unit):
        """
        Push a custom CloudWatch metric

        :type metric_name: str
        :param metric_name: Metric name

        :type value: float
        :param value: Value to push

        :type unit: str
        :param unit: Unit of measurement (e.g. Bytes, Count)

        :return: None
        """
        self.log_trace('Putting CloudWatch Custom Metric: {metric_name}...'.format(metric_name=metric_name))
        self.cloudwatch_client.put_metric(
            stage=self.get_stage(),
            namespace=self.get_aws_function_name(),
            metric_name=metric_name,
            value=value,
            unit=unit
        )

    def put_cloudwatch_count(self, metric_name):
        """
        Push a CloudWatch count metric

        :type metric_name: str
        :param metric_name: Metric name

        :return: None
        """
        self.log_trace('Putting CloudWatch Count Metric: {metric_name}...'.format(metric_name=metric_name))
        self.put_cloudwatch_custom_metric(
            metric_name=metric_name,
            value=1.0,
            unit='Count'
        )
