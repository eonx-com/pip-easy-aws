#!/usr/bin/env python
# -*- coding: utf-8 -*-

from EasyIterator.ClientError import ClientError
from EasyLog.Log import Log
from EasyFilesystem.S3.Bucket import Bucket as S3Bucket
from EasySecretsManager.Client import Client as SecretsManagerClient
from EasyFilesystem.Sftp.Server import Server as SftpServer
from time import strftime


class Client:
    FILESYSTEM_TYPE_S3 = 's3'
    FILESYSTEM_TYPE_SFTP = 'sftp'

    def __init__(self, sources=None, success_destinations=None, error_destinations=None):
        """
        Setup iterator

        :type sources: list or None
        :param sources: List of file sources to iterate

        :type success_destinations: list or None
        :param success_destinations: List of success destinations to iterate

        :type error_destinations: list or None
        :param error_destinations: List of error destinations to iterate

        :return: None
        """
        Log.trace('Instantiating Iterator...')

        self.__sources__ = []
        self.__success_destinations__ = []
        self.__error_destinations__ = []

        # Keep track of the current file being iterated
        self.__current_file__ = None

        # Keep a timestamp that will be used as the unique timestamp for folders created by this process
        self.__start_time__ = strftime("%Y-%m-%d %H:%M:%S")

        # If there are no source folder, throw an exception
        if sources is None or len(sources) == 0:
            Log.exception(ClientError.ERROR_SOURCE_LIST_EMPTY)

        # Add source folders
        Log.trace('Adding Sources...')
        for source in sources:
            self.__sources__.append(Client.create_filesystem(source))

        # Add success destination folders
        if success_destinations is not None:
            for success_destination in success_destinations:
                self.__success_destinations__.append(Client.create_filesystem(success_destination))

        # Add error destination folders
        if error_destinations is not None:
            Log.debug('Adding error destinations to iterator...')
            for error_destination in error_destinations:
                self.__error_destinations__.append(Client.create_filesystem(error_destination))

    def iterate_files(self, callback, maximum_files=None) -> None:
        """
        Iterate over files sourced from S3 buckets

        :type callback: function
        :param callback: Callback function that is executed as each file is downloaded

        :type maximum_files: int/None
        :param maximum_files: Maximum number of files to iterate over, if not set no limit is set

        :return: None
        """
        Log.trace('Iterating Files...')

        count_files = 0

        # Make sure all sources are expected types before we start iterating
        for source in self.__sources__:
            if not isinstance(source, S3Bucket) and not isinstance(source, SftpServer):
                raise Exception('Invalid iteration source supplied: {type}'.format(type=type(source)))

        # Iterate through each source
        for source in self.__sources__:
            # If we have reached the maximum number of files, break out of the loop
            if count_files >= maximum_files:
                Log.debug('File Limit Reached...')
                break

            files = source.iterate_files(callback=callback, maximum_files=maximum_files)
            count_files += len(files)

        Log.debug('Finished Iterating')

    @staticmethod
    def create_filesystem(source):
        """
        Convert input parameter to the appropriate filesystem

        :type source: dict
        :param source: The input parameters details

        :return: EasyS3Bucket or EasySftpServer
        """
        filesystem_type = str(source['type']).lower()

        if filesystem_type == Client.FILESYSTEM_TYPE_S3:
            Log.debug('Configuring S3 Bucket...')
            return Client.create_s3_bucket(source=source)
        elif filesystem_type == Client.FILESYSTEM_TYPE_SFTP:
            Log.debug('Configuring SFTP Server...')
            return Client.create_sftp_server(source=source)

        Log.exception(ClientError.ERROR_SOURCE_TYPE_INVALID)

    @staticmethod
    def create_s3_bucket(source) -> S3Bucket:
        """
        Create an S3 bucket from input parameters

        :type source: dict
        :param source: The input parameters

        :return: S3Bucket
        """
        Log.trace('Creating S3Bucket...')

        for parameter in ('bucket_name', 'base_path'):
            Log.debug('Checking Parameter: {parameter}'.format(parameter=parameter))
            if parameter not in source:
                Log.exception(ClientError.ERROR_SOURCE_CONFIG_INVALID)

        Log.debug('Bucket Name: {bucket_name}'.format(bucket_name=source['bucket_name']))
        Log.debug('Bucket Base Path: {base_path}'.format(base_path=source['base_path']))

        return S3Bucket(
            bucket_name=source['bucket_name'],
            base_path=source['base_path'],
        )

    @staticmethod
    def create_sftp_server(source) -> SftpServer:
        """
        Create an SFTP server from input parameters

        :type source: dict
        :param source: The input parameters

        :return: SftpServer
        """
        Log.trace('Creating SFTP Server...')

        Log.debug('Loading Secret...')
        if 'secret' not in source:
            Log.exception(ClientError.ERROR_SOURCE_CONFIG_INVALID)
        secret = SecretsManagerClient.get_secret(source['secret'])

        Log.debug('Validating Secret Fields...')
        parameters = {}
        for lookup in (
                'username_key',
                'password_key',
                'rsa_private_key_key',
                'fingerprint_key',
                'fingerprint_type_key',
                'path_key',
                'port_key'
        ):
            Log.debug('Checking Parameter: {lookup}'.format(lookup=lookup))
            if lookup in source:
                parameters[lookup] = secret[source[lookup]]
            else:
                parameters[lookup] = None

        Log.debug('Creating SftpServer...')
        return SftpServer(
            address=secret[source['address_key']],
            username=parameters['username_key'],
            password=parameters['password_key'],
            rsa_private_key=parameters['rsa_private_key_key'],
            fingerprint=parameters['fingerprint_key'],
            fingerprint_type=parameters['fingerprint_type_key'],
            port=parameters['port_key'],
            base_path=parameters['path_key']
        )
