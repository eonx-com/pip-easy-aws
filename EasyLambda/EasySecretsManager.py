#!/usr/bin/env python
# -*- coding: utf-8 -*-

import base64
import boto3
import json

from botocore.exceptions import ClientError
from EasyLambda.EasyLog import EasyLog


class EasySecretsManager:
    # Cache for SecretsManager client
    __client__ = None

    # Cache for retrieved secrets
    __secrets_cache__ = {}

    @staticmethod
    def get_secretsmanager_client():
        """
        Setup SecretsManager client
        """
        # If we haven't gotten a client yet- create one now and cache it for future calls
        if EasySecretsManager.__client__ is None:
            EasyLog.trace('Instantiating AWS Secrets Manager client...')
            EasySecretsManager.__client__ = boto3.session.Session().client('secretsmanager')

        # Return the cached client
        return EasySecretsManager.__client__

    @staticmethod
    def list_secrets() -> dict:
        """
        Return dictionary of all accessible secrets

        :return: dict
        """
        secrets = None

        EasyLog.trace('Listing secrets...')
        secrets_current = EasySecretsManager.__client__.list_secrets()

        # Retrieve all available secrets (it may be necessary to perform multiple requests
        while True:
            # Iterate through the content of the most recent search results
            for secret in secrets_current['SecretList']:
                if secrets is None:
                    secrets = {}

                description = None
                if 'Description' in secret:
                    description = secret['Description']

                secrets[secret['Name']] = {
                    'arn': secret['ARN'],
                    'description': description
                }

            # If we have found all of the secrets, exit out of the loop
            if 'NextToken' not in secrets_current:
                break

            # There were more results, perform another request to get the next set of results
            EasyLog.debug('Listing next secrets page...')
            secrets_current = EasySecretsManager.__client__.list_secrets(
                NextToken=secrets_current['NextToken']
            )

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
        EasyLog.trace('Retrieving secret: {name}...'.format(name=name))

        # If the secret has already been loaded and we are caching results, just return the existing secret
        if name in EasySecretsManager.__secrets_cache__:
            EasyLog.debug('Existing cached value found')

            if cache is True:
                EasyLog.trace('Returning cached secret')
                return EasySecretsManager.__secrets_cache__[name]
            else:
                EasyLog.debug('Clearing existing cached value...')
                EasySecretsManager.__secrets_cache__[name] = {}

        try:
            # Load requested secret from AWS Secrets Manager
            EasyLog.debug('Retrieving value from AWS Secrets Manager...')
            get_secret_value_response = EasySecretsManager.__client__.get_secret_value(SecretId=name)

            if 'SecretString' in get_secret_value_response:
                # Plain text JSON value detected
                EasyLog.debug('Found JSON value...')
                secret_json = get_secret_value_response['SecretString']
            else:
                # Base64 encoded JSON value detected
                EasyLog.debug('Found Base64 encoded value...')
                secret_json = base64.b64decode(get_secret_value_response['SecretBinary'])

            # JSON decode the secret and store it in the global dictionary
            EasyLog.debug('Caching value...')
            EasySecretsManager.__secrets_cache__[name] = json.loads(secret_json)

            # Return the secrets dictionary
            EasyLog.trace('Returning secret...')
            return EasySecretsManager.__secrets_cache__[name]

        except ClientError as client_exception:
            # If anything went wrong during this step log a meaningful error

            if client_exception.response['Error']['Code'] == 'ResourceNotFoundException':
                EasyLog.exception('The requested AWS Secrets Manager resource was not found', client_exception)
            elif client_exception.response['Error']['Code'] == 'InvalidRequestException':
                EasyLog.exception('The request to AWS Secrets Manager was invalid', client_exception)
            elif client_exception.response['Error']['Code'] == 'InvalidParameterException':
                EasyLog.exception('The request to AWS Secrets Manager had invalid parameters', client_exception)
            else:
                EasyLog.exception('An unhandled exception error occurred while attempting to load value from AWS Secrets Manager', client_exception)

            raise client_exception
