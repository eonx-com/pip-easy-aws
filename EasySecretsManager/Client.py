#!/usr/bin/env python
# -*- coding: utf-8 -*-

import base64
import boto3
import json

from botocore.exceptions import ClientError as BotoClientError
from EasyLog.Log import Log
from EasySecretsManager.ClientError import ClientError


class Client:
    # Cache for SecretsManager client
    __client__ = None

    # Cache for retrieved secrets
    __secrets_cache__ = {}

    @staticmethod
    def get_client():
        """
        Setup SecretsManager client
        """
        # If we haven't gotten a client yet- create one now and cache it for future calls
        if Client.__client__ is None:
            Log.trace('Instantiating AWS Secrets Manager client...')
            Client.__client__ = boto3.session.Session().client('secretsmanager')

        # Return the cached client
        return Client.__client__

    @staticmethod
    def secret_exists(name) -> bool:
        """
        Return boolean flag indicating whether the specified secret can be located

        :type name: str
        :param name: Name of the secret to test

        :return: bool
        """
        list_secrets_result = None

        try:
            list_secrets_result = Client.list_secrets()
        except Exception as list_secrets_exception:
            Log.exception(ClientError.ERROR_SECRET_LIST_UNHANDLED_EXCEPTION, list_secrets_exception)

        return name in list_secrets_result

    @staticmethod
    def list_secrets() -> dict:
        """
        Return dictionary of all accessible secrets

        :return: dict
        """
        secrets = None

        Log.trace('Listing secrets...')
        secrets_current = Client.get_client().list_secrets()

        # Retrieve all available secrets (it may be necessary to perform multiple requests
        while True:
            # Iterate through the content of the most recent search results
            for secret in secrets_current['SecretList']:
                if secrets is None:
                    secrets = {}

                description = None
                if 'Description' in secret:
                    description = secret['Description']

                secrets[secret['Name']] = {'arn': secret['ARN'], 'description': description}

            # If we have found all of the secrets, exit out of the loop
            if 'NextToken' not in secrets_current:
                break

            # There were more results, perform another request to get the next set of results
            Log.debug('Listing next secrets page...')
            secrets_current = Client.get_client().list_secrets(NextToken=secrets_current['NextToken'])

        # Return the secrets we found
        return secrets

    @staticmethod
    def get_secret(name, cache=True) -> dict:
        """
        Get the requested secrets value

        :type name: string
        :param name: The name of the secret to be retrieved

        :type cache: bool
        :param cache: If True, and the requested secret been retrieved from AWS then the cached value will be returned

        :return: dict
        """
        Log.trace('Retrieving secret: {name}...'.format(name=name))

        # If the secret has already been loaded and we are caching results, just return the existing secret
        if name in Client.__secrets_cache__:
            Log.debug('Existing cached value found')

            if cache is True:
                Log.trace('Returning cached secret')
                return Client.__secrets_cache__[name]
            else:
                Log.debug('Clearing existing cached value...')
                Client.__secrets_cache__[name] = {}

        try:
            # Load requested secret from AWS Secrets Manager
            Log.debug('Retrieving value from AWS Secrets Manager...')
            get_secret_value_response = Client.get_client().get_secret_value(SecretId=name)

            if 'SecretString' in get_secret_value_response:
                # Plain text JSON value detected
                Log.debug('Found JSON value...')
                secret_json = get_secret_value_response['SecretString']
            else:
                # Base64 encoded JSON value detected
                Log.debug('Found Base64 encoded value...')
                secret_json = base64.b64decode(get_secret_value_response['SecretBinary'])

            # JSON decode the secret and store it in the global dictionary
            Log.debug('Caching value...')
            Client.__secrets_cache__[name] = json.loads(secret_json)

            # Return the secrets dictionary
            Log.trace('Returning secret...')
            return Client.__secrets_cache__[name]

        except BotoClientError as client_exception:
            # If anything went wrong during this step log a meaningful error

            if client_exception.response['Error']['Code'] == 'ResourceNotFoundException':
                Log.exception('The requested AWS Secrets Manager resource was not found', client_exception)
            elif client_exception.response['Error']['Code'] == 'InvalidRequestException':
                Log.exception('The request to AWS Secrets Manager was invalid', client_exception)
            elif client_exception.response['Error']['Code'] == 'InvalidParameterException':
                Log.exception('The request to AWS Secrets Manager had invalid parameters', client_exception)
            else:
                Log.exception('An unhandled base_exception error occurred while attempting to load value from AWS Secrets Manager', client_exception)

            raise client_exception

    @staticmethod
    def create_secret(name, value) -> None:
        """
        Create a secret

        :type name: str
        :param name: Name of the secret

        :type value: dict
        :param value: Secret value

        :return: None
        """
        if Client.secret_exists(name) is True:
            Log.exception(ClientError.ERROR_SECRET_CREATE_ALREADY_EXISTS)

        try:
            Client.get_client().create_secret(Name=name, SecretString=json.dumps(value))
        except Exception as create_exception:
            Log.exception(ClientError.ERROR_SECRET_CREATE_UNHANDLED_EXCEPTION, create_exception)

    @staticmethod
    def delete_secret(name, recovery_window=7, force_delete=False) -> None:
        """
        Delete an existing secret

        :type name: str
        :param name: Name of the secret

        :type recovery_window: int
        :param recovery_window: Number of days to retain secret after deletion

        :type force_delete: bool
        :type force_delete: Boolean flag to allow immediate deletion without grace window

        :return: None
        """
        if Client.secret_exists(name) is False:
            Log.exception(ClientError.ERROR_SECRET_DELETE_NOT_FOUND)

        try:
            if force_delete is True:
                Client.get_client().delete_secret(SecretId=name, ForceDeleteWithoutRecovery=True)
            else:
                Client.get_client().delete_secret(SecretId=name, RecoveryWindowInDays=recovery_window)
        except Exception as delete_exception:
            Log.exception(ClientError.ERROR_SECRET_DELETE_UNHANDLED_EXCEPTION, delete_exception)

    @staticmethod
    def update_secret(name, value) -> None:
        """
        Update an existing secret

        :type name: str
        :param name: Name of the secret

        :type value: dict
        :param value: Secret value

        :return: None
        """
        if Client.secret_exists(name) is False:
            Log.exception(ClientError.ERROR_SECRET_UPDATE_NOT_FOUND)

        try:
            Client.get_client().update_secret(SecretId=name, SecretString=json.dumps(value))
        except Exception as create_exception:
            Log.exception(ClientError.ERROR_SECRET_UPDATE_UNHANDLED_EXCEPTION, create_exception)
