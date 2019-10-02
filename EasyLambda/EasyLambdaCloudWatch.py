from EasyLambda.EasyLambdaSession import EasyLambdaSession


class EasyLambdaCloudWatch(EasyLambdaSession):
    """
    Easy Lambda CloudWatch class
    """

    def __init__(self, aws_event, aws_context):
        """
        :type aws_event: dict
        :param aws_event: AWS Lambda uses this parameter to pass in event data to the handler

        :type aws_context: LambdaContext
        :param aws_context: AWS Lambda uses this parameter to provide runtime information to your handler

        :return: None
        """
        super(EasyLambdaCloudWatch, self).__init__(aws_event=aws_event, aws_context=aws_context)

        self.__aws_context__ = aws_context
        self.__aws_event__ = aws_event
        self.__cloudwatch_client__ = self.get_easy_session_manager().get_cloudwatch_client()

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
        self.__cloudwatch_client__.put_metric(
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
        self.put_cloudwatch_custom_metric(
            metric_name=metric_name,
            value=1.0,
            unit='Count'
        )
