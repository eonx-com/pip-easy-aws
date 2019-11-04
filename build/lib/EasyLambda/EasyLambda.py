#!/usr/bin/env python
# -*- coding: utf-8 -*-

from abc import abstractmethod
from typing import Optional

from EasyCloudWatch.Client import Client as CloudWatch
from EasySlack.Client import Client as Slack
from EasyGenie.Client import Client as Genie
from EasyLog.Log import Log


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

        Log.set_function_name(self.get_aws_function_name())

        # Set log level
        if 'log_level' in self.__aws_event__:
            print('Retrieving requested logging level from Lambda function parameters...')
            # noinspection PyBroadException
            try:
                log_level = int(self.__aws_event__['log_level'])
                print('Requested logging level: {log_level}'.format(log_level=Log.get_log_level_name(log_level)))
                Log.set_level(log_level)
            except Exception as log_exception:
                print('An unexpected error occurred while attempting to set desired logging level.')
                raise Exception(log_exception)

        if 'slack_log_level' in self.__aws_event__:
            print('Retrieving requested Slack logging level from Lambda function parameters...')
            # noinspection PyBroadException
            try:
                slack_log_level = int(self.__aws_event__['slack_log_level'])
                print('Requested logging level: {slack_log_level}'.format(slack_log_level=Log.get_log_level_name(slack_log_level)))
                Log.set_slack_level(slack_log_level)
            except Exception as log_exception:
                print('An unexpected error occurred while attempting to set desired Slack logging level.')
                raise Exception(log_exception)

        if 'slack_token' in self.__aws_event__ and 'slack_channel' in self.__aws_event__:
            if str(self.__aws_event__['slack_token']).strip() != '' and str(self.__aws_event__['slack_channel']).strip() != '':
                Log.info('Setting up Slack client...')
                self.__slack__ = Slack(token=self.__aws_event__['slack_token'])
                Log.set_slack_client(slack=self.__slack__, channel=self.__aws_event__['slack_channel'])

        if 'genie_log_level' in self.__aws_event__:
            print('Retrieving requested OpsGenie logging level from Lambda function parameters...')
            # noinspection PyBroadException
            try:
                genie_log_level = int(self.__aws_event__['genie_log_level'])
                print('Requested logging level: {genie_log_level}'.format(genie_log_level=Log.get_log_level_name(genie_log_level)))
                Log.set_genie_level(genie_log_level)
            except Exception as log_exception:
                print('An unexpected error occurred while attempting to set desired Genie logging level.')
                raise Exception(log_exception)

        if 'genie_key' in self.__aws_event__ and 'genie_team' in self.__aws_event__ and 'genie_alias' in self.__aws_event__:
            if str(self.__aws_event__['genie_key']).strip() != '' and str(self.__aws_event__['genie_team']).strip() != '':
                Log.info('Setting up Genie client...')
                self.__genie__ = Genie(key=self.__aws_event__['genie_key'])
                Log.set_genie_client(
                    genie=self.__genie__,
                    team=self.__aws_event__['genie_team'],
                    alias=self.__aws_event__['genie_alias']
                )

        try:
            Log.trace('Executing user initialization function...')
            self.init()
        except Exception as init_exception:
            # Something went wrong inside the users init function- log the error
            Log.exception('Unhandled base_exception during execution of user initialization function', init_exception)
            raise init_exception

        try:
            Log.trace('Executing user run function...')
            self.run()
        except Exception as run_exception:
            # Something went wrong inside the users run function- log the error
            Log.error('Unhandled base_exception during execution of user run function:\n{run_exception}'.format(run_exception=run_exception))
            raise run_exception

        # Execution completed, log out the time remaining- this may be useful for tracking bloat/performance degradation over the life of the Lambda function
        time_remaining = self.get_aws_time_remaining()
        Log.info('Execution completed with {time_remaining} seconds remaining'.format(time_remaining=time_remaining / 1000))
        CloudWatch.put_metric('lambda_time_remaining', time_remaining, 'Milliseconds')

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

    def get_slack_client(self) -> Optional[Slack]:
        """
        Return Slack client if available

        :return: Slack client (if token was provided) or None
        """
        return self.__slack__
