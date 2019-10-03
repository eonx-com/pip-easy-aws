from EasyBoto3.EasySessionManager import EasySessionManager

from EasyLambda import EasyLambda
from EasyLambda.EasyLambdaLog import EasyLambdaLog


class EasyLambdaSession(EasyLambdaLog):
    def __init__(
            self,
            aws_event,
            aws_context
    ):
        """
        :type aws_event: dict
        :param aws_event: AWS Lambda uses this parameter to pass in event data to the handler

        :type aws_context: LambdaContext
        :param aws_context: AWS Lambda uses this parameter to provide runtime information to your handler

        :return: None
        """
        super(EasyLambda.EasyLambdaSession, self).__init__(aws_event=aws_event, aws_context=aws_context)

        self.__aws_context__ = aws_context
        self.__aws_event__ = aws_event
        self.__aws_stage__ = self.get_aws_event_parameter('stage')

        # Set logging level based on function parameters
        if 'log_level' in self.__aws_event__:
            self.set_log_level(log_level=int(self.__aws_event__['log_level']))

        self.__easy_session_manager__ = EasySessionManager(log_level=self.get_log_level())

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

    def get_aws_stage(self) -> str:
        """
        Return the deployment stage this function belongs to

        :return: str
        """
        return self.__aws_stage__

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
        self.log_trace('Retrieving AWS execution time remaining...')
        return self.__aws_context__.get_remaining_time_in_millis()

    def get_easy_session_manager(self) -> EasySessionManager:
        """
        Return the AWS session manager

        :return: EasySessionManager
        """
        return self.__easy_session_manager__
