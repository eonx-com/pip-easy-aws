#!/usr/bin/env python
# -*- coding: utf-8 -*-

import boto3
import os

from EasyLog.Log import Log


class EasyCloudWatch:
    # Cache for CloudWatch client
    __client__ = None

    @staticmethod
    def get_cloudwatch_client():
        """
        Setup CloudWatch client
        """
        # If we haven't gotten a client yet- create one now and cache it for future calls
        if EasyCloudWatch.__client__ is None:
            Log.trace('Instantiating AWS CloudWatch client...')
            EasyCloudWatch.__client__ = boto3.session.Session().client('cloudwatch')

        # Return the cached client
        return EasyCloudWatch.__client__

    @staticmethod
    def put_metric(metric_name, value, unit):
        """
        Push a CloudWatch metric

        :type metric_name: str
        :param metric_name: Metric name

        :type value: float
        :param value: Value to save

        :type unit: str
        :param unit: Unit of measurement (e.g. Bytes, Count)

        :return: None
        """
        Log.trace('Putting AWS CloudWatch metric: {metric_name}...'.format(metric_name=metric_name))
        EasyCloudWatch.get_cloudwatch_client().put_metric_data(
            Namespace=os.environ.get('AWS_LAMBDA_FUNCTION_NAME'),
            MetricData=[{'MetricName': metric_name, 'Unit': unit, 'Value': value}]
        )

    @staticmethod
    def increment_count(metric_name):
        """
        Increment a count CloudWatch metric

        :type metric_name: str
        :param metric_name: Metric name

        :return: None
        """
        Log.trace('Incrementing AWS CloudWatch counter: {metric_name}...'.format(metric_name=metric_name))
        EasyCloudWatch.get_cloudwatch_client().put_metric_data(
            Namespace=os.environ.get('AWS_LAMBDA_FUNCTION_NAME'),
            MetricData=[{'MetricName': metric_name, 'Unit': 'Count', 'Value': 1.0}]
        )
