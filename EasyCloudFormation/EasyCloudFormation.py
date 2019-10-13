#!/usr/bin/env python
# -*- coding: utf-8 -*-

import boto3

from botocore.exceptions import ClientError
from EasyLog.Log import Log
from time import sleep


class EasyCloudFormation:
    # Active stack status constants
    ACTIVE_STACK_STATUS = [
        'CREATE_IN_PROGRESS',
        'CREATE_FAILED',
        'CREATE_COMPLETE',
        'ROLLBACK_IN_PROGRESS',
        'ROLLBACK_FAILED',
        'ROLLBACK_COMPLETE',
        'UPDATE_IN_PROGRESS',
        'UPDATE_COMPLETE_CLEANUP_IN_PROGRESS',
        'UPDATE_COMPLETE',
        'UPDATE_ROLLBACK_IN_PROGRESS',
        'UPDATE_ROLLBACK_FAILED',
        'UPDATE_ROLLBACK_COMPLETE_CLEANUP_IN_PROGRESS',
        'UPDATE_ROLLBACK_COMPLETE',
        'REVIEW_IN_PROGRESS'
    ]

    # Error constants

    ERROR_INVALID_RESPONSE = 'An unexpected response was received from AWS CloudFormation request.'
    ERROR_STACK_NOT_FOUND = 'The requested stack could not be found, please check IAM permissions assigned to the function.'
    ERROR_DRIFT_DETECTION_FAILED = 'An unexpected error occurred while attempting to perform drift detection.'
    ERROR_DRIFT_DETECTION_CONFLICT = 'CloudFormation reported that drift detection was already in progress for the requested stack, and it was not requested by this application.'

    # Cache drift detection request IDs
    __drift_request_ids__ = {}

    # Cache for CloudFormation client
    __client__ = None

    @staticmethod
    def get_cloudformation_client():
        """
        Setup CloudWatch client
        """
        # If we haven't gotten a client yet- create one now and cache it for future calls
        if EasyCloudFormation.__client__ is None:
            Log.trace('Instantiating AWS Secrets Manager client...')
            EasyCloudFormation.__client__ = boto3.session.Session().client('cloudformation')

        # Return the cached client
        return EasyCloudFormation.__client__

    @staticmethod
    def list_stacks() -> list:
        """
        Retrieve a list of all available CloudFormation stacks

        :return: list
        """
        Log.trace('Listing stacks...')
        response = EasyCloudFormation.get_cloudformation_client().list_stacks(EasyCloudFormation.ACTIVE_STACK_STATUS)

        if 'StackSummaries' not in response:
            raise Exception(EasyCloudFormation.ERROR_INVALID_RESPONSE)

        stacks = response['StackSummaries']

        while stacks is None or 'NextToken' not in response.keys():
            response = EasyCloudFormation.get_cloudformation_client().list_stacks(EasyCloudFormation.ACTIVE_STACK_STATUS)
            if 'StackSummaries' not in response:
                raise Exception(EasyCloudFormation.ERROR_INVALID_RESPONSE)
            stacks.append(response['StackSummaries'])

        return stacks

    @staticmethod
    def check_stack_drift(stack_name, max_attempts=3, attempt_interval=3):
        """
        Check if the requested stack has drifted

        :type stack_name: str
        :param stack_name: Name of the stack to check has drifted

        :type max_attempts: int
        :param max_attempts: The maximum number of attempts to wait for drift detection

        :type attempt_interval: int
        :param attempt_interval: Number of seconds between attempts

        :return: bool
        """
        Log.trace('Checking drift on stack: {stack_name}...'.format(stack_name=stack_name))

        drift_status = None
        attempt_count = 0
        response = {}

        Log.debug('Request drift detection...')
        request_id = EasyCloudFormation.detect_stack_drift(stack_name)

        while drift_status is None or drift_status == 'DETECTION_IN_PROGRESS' and attempt_count <= max_attempts:
            Log.debug('Polling drift detection status...')
            response = EasyCloudFormation.get_cloudformation_client().describe_stack_drift_detection_status(StackDriftDetectionId=request_id)

            # Make sure we received the expected response
            if 'DetectionStatus' not in response:
                Log.error('Could not find expected `DetectionStatus` key in response from AWS CloudFormation client')
                raise Exception(EasyCloudFormation.ERROR_INVALID_RESPONSE)

            drift_status = response['DetectionStatus']

            if drift_status == 'DETECTION_IN_PROGRESS':
                Log.debug('Sleeping {attempt_interval} seconds...'.format(attempt_interval=attempt_interval))
                attempt_count += 1
                sleep(attempt_interval)

        Log.debug('Finished polling for stack drift...')
        Log.debug('Last detection status: {drift_status}'.format(drift_status=drift_status))

        if drift_status != 'DETECTION_IN_PROGRESS':
            # If drift detection finished clear the cached request ID
            EasyCloudFormation.__drift_request_ids__[stack_name] = None

        # If detection failed, try to display a meaningful error
        if drift_status == 'DETECTION_FAILED':
            # Make sure we received the expected response
            if 'DetectionStatusReason' not in response:
                Log.error('Could not find expected `DetectionStatusReason` key in response from AWS CloudFormation client')
                raise Exception(EasyCloudFormation.ERROR_INVALID_RESPONSE)

            Log.error('Drift detection failed: {reason}'.format(reason=response['DetectionStatusReason']))

            if 'Failed to detect drift on resource' not in response['DetectionStatusReason']:
                Log.error('Drift detection failed for the following reason: {reason}'.format(reason=response['DetectionStatusReason']))
                raise Exception(EasyCloudFormation.ERROR_DRIFT_DETECTION_FAILED)

                # Return the drift status
        return drift_status

    @staticmethod
    def detect_stack_drift(stack_name) -> str:
        """
        Start an asynchronous drift check on the specified stack, returns the request ID

        :type stack_name: str
        :param stack_name: Name of the stack to check has drifted

        :return: str
        """
        Log.trace('Requesting drift check on stack: {stack_name}'.format(stack_name=stack_name))

        # Make sure the stack name exists and can be seen by the client
        Log.debug('Checking requested stack exists...')
        stacks = EasyCloudFormation.list_stacks()

        if stack_name not in stacks:
            Log.error(EasyCloudFormation.ERROR_STACK_NOT_FOUND)
            raise Exception(EasyCloudFormation.ERROR_STACK_NOT_FOUND)

        try:
            # Start drift detect detection request- this is asynchronous and we will have to poll it later to check the status
            Log.debug('Requesting drift status check...')
            response = EasyCloudFormation.get_cloudformation_client().detect_stack_drift(StackName=stack_name)

            # Make sure we received the expected result
            if 'StackDriftDetectionId' not in response:
                raise Exception(EasyCloudFormation.ERROR_INVALID_RESPONSE)

            Log.debug('Drift check started: {request_id}'.format(request_id=response['StackDriftDetectionId']))
            return response['StackDriftDetectionId']
        except ClientError as detect_exception:
            if 'Drift detection is already in progress for stack' in detect_exception.response['Error']['Message']:
                Log.debug('Drift detection already in progress for this stack')
                # Drift detection is already in progress, return last request ID if we have one
                if stack_name in EasyCloudFormation.__drift_request_ids__:
                    if EasyCloudFormation.__drift_request_ids__[stack_name] is not None:
                        # Return the previous drift detection requests ID
                        Log.debug('Returning cached request ID: {request_id}'.format(request_id=EasyCloudFormation.__drift_request_ids__[stack_name]))
                        return EasyCloudFormation.__drift_request_ids__[stack_name]

                # Otherwise another process must be performing the check- raise an base_exception as this is a potential conflict of interest
                Log.error('Drift detection was already initiated by external process')
                raise Exception(EasyCloudFormation.ERROR_DRIFT_DETECTION_CONFLICT)

            # Something else went wrong
            Log.error('An unexpected error occurred while checking for stack drift: {detect_exception}'.format(detect_exception=detect_exception))
            raise Exception(EasyCloudFormation.ERROR_DRIFT_DETECTION_FAILED)
