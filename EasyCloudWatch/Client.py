#!/usr/bin/env python
# -*- coding: utf-8 -*-

import boto3
import os

from EasyLog.Log import Log


class Client:
    # Allow metrics
    UNIT_SECONDS = 'Seconds'
    UNIT_MICROSECONDS = 'Microseconds'
    UNIT_MILLISECONDS = 'Milliseconds'
    UNIT_BYTES = 'Bytes'
    UNIT_KILOBYTES = 'Kilobytes'
    UNIT_MEGABYTES = 'Megabytes'
    UNIT_GIGABYTES = 'Gigabytes'
    UNIT_TERABYTES = 'Terabytes'
    UNIT_BITS = 'Bits'
    UNIT_KILOBITS = 'Kilobits'
    UNIT_MEGABITS = 'Megabits'
    UNIT_GIGABITS = 'Gigabits'
    UNIT_TERABITS = 'Terabits'
    UNIT_PERCENT = 'Percent'
    UNIT_COUNT = 'Count'
    UNIT_BYTES_PER_SECOND = 'Bytes / Second'
    UNIT_KILOBYTES_PER_SECOND = 'Kilobytes / Second'
    UNIT_MEGABYTES_PER_SECOND = 'Megabytes / Second'
    UNIT_GIGABYTES_PER_SECOND = 'Gigabytes / Second'
    UNIT_TERABYTES_PER_SECOND = 'Terabytes / Second'
    UNIT_BITS_PER_SECOND = 'Bits / Second'
    UNIT_KILOBITS_PER_SECOND = 'Kilobits / Second'
    UNIT_MEGABITS_PER_SECOND = 'Megabits / Second'
    UNIT_GIGABITS_PER_SECOND = 'Gigabits / Second'
    UNIT_TERABITS_PER_SECOND = 'Terabits / Second'
    UNIT_COUNT_PER_SECOND = 'Count / Second'

    # Cache for CloudWatch client
    __client__ = None

    @staticmethod
    def get_cloudwatch_client():
        """
        Setup CloudWatch client
        """
        # If we haven't gotten a client yet- create one now and cache it for future calls
        if Client.__client__ is None:
            Log.trace('Instantiating AWS CloudWatch Client...')
            Client.__client__ = boto3.session.Session().client('cloudwatch')

        # Return the cached client
        return Client.__client__

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
        Client.get_cloudwatch_client().put_metric_data(
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
        Client.get_cloudwatch_client().put_metric_data(
            Namespace=os.environ.get('AWS_LAMBDA_FUNCTION_NAME'),
            MetricData=[{'MetricName': metric_name, 'Unit': Client.UNIT_COUNT, 'Value': 1.0}]
        )
