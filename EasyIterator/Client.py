#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os

from time import strftime
from EasyIterator.ClientError import ClientError
from EasyLog.Log import Log

from EasyLocalDisk.Client import Client as LocalDiskClient
from EasyLocalDisk.File import File as LocalDiskFile

from EasySftp.Client import Client as SftpClient
from EasySftp.Server import Server as SftpServer
from EasySftp.File import File as SftpFile

from EasyS3.Client import Client as S3Client
from EasyS3.Bucket import Bucket as S3Bucket
from EasyS3.File import File as S3File


class Iterator:
    def __init__(
            self,
            sources=None,
            success_destinations=None,
            error_destinations=None
    ):
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
        Log.debug('Adding source locations to iterator...')
        self.add_sources(sources)

        # Add success destination folders
        if success_destinations is not None:
            Log.debug('Adding success destinations to iterator...')
            self.add_success_destinations(success_destinations)
        else:
            Log.warning('No success destinations were specified, files will not be moved on completion')

        # Add error destination folders
        if error_destinations is not None:
            Log.debug('Adding error destinations to iterator...')
            self.add_errors_destinations(error_destinations)
        else:
            Log.warning('No error destinations were specified, failed files may remain in source folder after execution')

    def add_sources(self, sources) -> None:
        """
        Add multiple input sources

        :type sources: list
        :param sources: List of sources to add

        :return: None
        """
        Log.trace('Adding file sources...')
        for source in sources:
            self.__sources__.append(self.__create_filesystem__(source))

    def add_errors_destinations(self, error_destinations) -> None:
        """
        Add multiple error destinations

        :type error_destinations: list
        :param error_destinations: List of error destinations to add

        :return: None
        """
        Log.trace('Adding error destinations...')
        for error_destination in error_destinations:
            self.__error_destinations__.append(self.__create_filesystem__(error_destination))

    def add_success_destinations(self, success_destinations) -> None:
        """
        Add multiple success destinations

        :type success_destinations: list
        :param success_destinations: List of success destinations to add

        :return: None
        """
        Log.trace('Adding success destinations...')
        for success_destination in success_destinations:
            self.__success_destinations__.append(self.__create_filesystem__(success_destination))

    def replace_current_file(self, local_filename):
        """
        Overwrite the file currently being iterated with the new files contents

        :type local_filename: str
        :param local_filename: The name of the file on the local filesystem to be uploaded in its place

        :return: None
        """
        Log.trace('Replacing Current File...')
        self.__current_file__.replace(local_filename)

    def iterate_files(
            self,
            callback,
            maximum_files=None
    ):
        files_processed = []
        count_files = 0

        """
        Iterate over files sourced from S3 buckets

        :type callback: function
        :param callback: Callback function that is executed as each file is downloaded

        :type maximum_files: int/None
        :param maximum_files: Maximum number of files to iterate over, if not set no limit is set

        :return: None
        """
        Log.trace('Iterating Files...')

        # Make sure all sources are expected types before we start iterating
        for source in self.__sources__:
            if not isinstance(source, S3Bucket) and not isinstance(source, SftpServer):
                raise Exception('Invalid iteration source supplied: {type}'.format(type=type(source)))

        # Maintain list of files we processed
        files_processed = []

        # Iterate through each source
        for source in self.__sources__:
            # If we have reached the maximum number of files, break out of the loop
            if count_files >= maximum_files:
                Log.debug('File Limit Reached...')
                break

            files = None

            if isinstance(source, SftpServer):
                files = self.__iterate_sftp_files__(source=source, callback=callback, maximum_files=maximum_files, count_files=count_files)
            elif isinstance(source, S3Bucket):
                files = self.__iterate_s3_files__(source=source, callback=callback, maximum_files=maximum_files, count_files=count_files)

            if files is not None:
                count_files += len(files)
                files_processed.extend(files)

        Log.debug('Finished Iterating...')
        return files_processed

    def __iterate_sftp_files__(self, source, callback, maximum_files, count_files):
        """
        Iterate files from an SFTP server

        :param source:
        :param callback:
        :param maximum_files:

        :return: list
        """
        Log.trace('Iterating SFTP server...')

        if maximum_files is None:
            staked_files = self.__stake_sftp_server__(source)
        else:
            remaining_files = maximum_files - count_files
            staked_files = self.__stake_sftp_server__(source, maximum_files=remaining_files)

        error = False

        try:
            for local_filename in staked_files:
                self.__current_file__ = local_filename

                if os.path.exists(self.__current_file__) is False:
                    Log.error('The current file could not be located on the local filesystem, skipping...')
                    continue

                try:
                    Log.debug('Current filename: {current_file}'.format(current_file=self.__current_file__))

                    # Trigger the users callback for each staked file
                    Log.debug('Triggering user callback function...')
                    callback(
                        file=self.__current_file__,
                        base_path=self.__download_path__,
                        easy_iterator=self
                    )

                    Log.debug('User callback function completed...')

                    # Check if there were success destinations defined
                    if self.__success_destinations__ is not None and len(self.__success_destinations__) > 0:
                        # Move file to success destinations
                        Log.debug('Moving to success destination(s)...')
                        self.__move_file__(
                            source=source,
                            destinations=self.__success_destinations__,
                            file=self.__current_file__
                        )
                    else:
                        # No success destinations were defined, issue a warning
                        Log.warning('No success destination(s) were defined, files will remain in source folder...')

                except Exception as file_exception:
                    error = True
                    Log.error(file_exception)
                    Log.error('An unexpected exception error during iteration of current file...')

                    # Check if there were error destinations defined
                    if self.__error_destinations__ is not None and len(self.__error_destinations__) > 0:
                        try:
                            Log.error('Attempting to move current file to error destination(s)...')
                            self.__move_file__(
                                source=source,
                                destinations=self.__error_destinations__,
                                file=self.__current_file__,
                                base_path=self.__start_time__
                            )
                            Log.error('Move to error destinations completed successfully...')
                        except Exception as cleanup_exception:
                            # The move to the error destination failed, log an error message
                            Log.error(cleanup_exception)
                            Log.error('An unexpected exception error occurred during movement of files to error destination, failed file may remain in source folder...')

                    else:
                        # No error destinations were defined, issue a warning
                        Log.warning('No error destination(s) were defined, failed files will remain in source folder...')

        except Exception as final_exception:
            Log.error(final_exception)
            Log.error('An unexpected exception error during iteration of current file...')

        if error is True:
            Log.error('One or more errors occurred while attempting to iterate the current file, please review the log messages for more information, manual intervention may be required...')

        # Return files we attempted to iterate
        return staked_files

    def __iterate_s3_files__(self, source, callback, maximum_files, count_files):
        # Iterate files from S3 bucket
        Log.debug('Iterating files from S3 bucket...')
        Log.debug('Bucket Name: {bucket_name}'.format(bucket_name=source.get_bucket_name()))
        Log.debug('Base Path: {base_path}'.format(base_path=source.get_base_path()))

        if maximum_files is None:
            # No file limit
            Log.debug('No file limit set, staking all available files...')
            staked_files = self.__stake_s3_bucket__(source)
        else:
            # Ensure we don't download more than the maximum files (less the number already downloaded)
            remaining_files = maximum_files - count_files
            Log.debug('File limit set, maximum of {remaining_files} file(s) remaining...'.format(remaining_files=remaining_files))
            staked_files = self.__stake_s3_bucket__(source, maximum_files=remaining_files)

        # Trigger the users callback for each staked file
        error = False

        try:
            for staked_file in staked_files:
                self.__current_file__ = staked_file

                try:
                    Log.debug('Current filename: {current_file}'.format(current_file=self.__current_file__.get_filename()))

                    if isinstance(staked_file, EasyS3File):
                        # Trigger the users callback for each staked file
                        Log.debug('Triggering user callback function...')
                        callback(
                            file=self.__current_file__,
                            base_path=self.__download_path__,
                            easy_iterator=self
                        )

                        # Check if there were success destinations defined
                        if self.__success_destinations__ is not None and len(self.__success_destinations__) > 0:
                            # Move file to success destinations
                            Log.debug('Moving to success destination(s)...')
                            self.__move_file__(
                                source=source,
                                destinations=self.__success_destinations__,
                                file=self.__current_file__
                            )
                        else:
                            # No success destinations were defined, issue a warning
                            Log.warning('No success destination(s) were defined, files will remain in source folder...')

                    else:
                        Log.error('Unexpected file type while iterating S3 bucekt...')
                        continue

                except Exception as file_exception:
                    error = True
                    Log.error(file_exception)
                    Log.error('An unexpected exception error during iteration of current file...')

                    # Check if there were error destinations defined
                    if self.__error_destinations__ is not None and len(self.__error_destinations__) > 0:
                        try:
                            Log.error('Attempting to move current file to error destination(s)...')
                            self.__move_file__(
                                source=source,
                                destinations=self.__error_destinations__,
                                file=self.__current_file__,
                                base_path=self.__start_time__
                            )
                            Log.error('Move to error destinations completed successfully...')
                        except Exception as cleanup_exception:
                            # The move to the error destination failed, log an error message
                            Log.error(cleanup_exception)
                            Log.error('An unexpected exception error occurred during movement of files to error destination, failed file may remain in source folder...')

                    else:
                        # No error destinations were defined, issue a warning
                        Log.warning('No error destination(s) were defined, failed files will remain in source folder...')

        except Exception as final_exception:
            Log.error(final_exception)
            Log.error('An unexpected exception error during iteration of current file...')

        if error is True:
            Log.error('One or more errors occurred while attempting to iterate the current file, please review the log messages for more information, manual intervention may be required...')

        # Return files we attempted to iterate
        return staked_files

    def __create_filesystem__(self, source):
        """
        Convert Lambda input parameter to one of either EasyS3Bucket of EasySftpServer for use by this class

        :type source: dict
        :param source: The input parameters details

        :return: EasyS3Bucket or EasySftpServer
        """
        if str(source['type']).lower() == 's3':
            # Create an EasyS3Bucket source
            Log.debug('Adding S3 file bucket source...')
            return self.__create_easy_s3_bucket__(source=source)
        elif str(source['type']).lower() == 'sftp':
            # Create an EasySftpServer source
            Log.debug('Adding SFTP server source...')
            return self.__create_easy_sftp_server__(source=source)
        else:
            # Unknown source type specified
            Log.error('Unknown file source specified: {type}'.format(type=source['type']))
            raise Exception('Unknown source type value specified, must be one of "s3" or "sftp"')

    def __create_easy_s3_bucket__(self, source) -> EasyS3Bucket:
        """
        Create an EasyS3Bucket object from the Lambda functions input variables

        :type source: dict
        :param source: The input parameters details

        :return: EasyS3Bucket
        """
        Log.debug('Creating EasyS3Bucket object...')

        # Make sure the file source contains required S3 bucket name and path

        error = False

        for parameter in ('bucket_name', 'base_path'):
            if parameter not in source:
                error = True
                Log.error('The supplied S3 source does not contain required "{parameter}" value, please check your function configuration'.format(
                    parameter=parameter
                ))

        if error is True:
            raise Exception('One or more required parameters was not set correctly')

        Log.debug('Bucket Name: {bucket_name}'.format(bucket_name=source['bucket_name']))
        Log.debug('Bucket Base Path: {base_path}'.format(base_path=source['base_path']))

        return self.__easy_session_manager__.get_s3_bucket_client(
            bucket_name=source['bucket_name'],
            base_path=source['base_path'],
        )

    def __create_easy_sftp_server__(self, source) -> EasySftpServer:
        """
        Create an EasySftpServer object from the Lambda functions input variables

        :type source: dict
        :param source: The input parameters details

        :return: EasySftpServer
        """
        Log.debug('Loading SFTP server credentials from secret: {secret}'.format(secret=source['secret']))
        secret = self.__easy_secrets_manager__.get_secret(source['secret'])

        Log.debug('Validating secret fields...')
        parameters = {}
        for key in ('username', 'password', 'rsa_private_key', 'fingerprint', 'fingerprint_type', 'path', 'port'):
            lookup = key + '_key'
            Log.debug('Searching for: {lookup}...'.format(lookup=lookup))
            if lookup in source:
                Log.debug('Secret value found')
                parameters[lookup] = secret[source[lookup]]
            else:
                Log.warning('Secret value not found')
                parameters[lookup] = None

        Log.debug('SFTP Server: {address}'.format(address=secret[source['address_key']]))
        Log.debug('SFTP Server Port Number: {port}'.format(port=parameters['port_key']))
        Log.debug('SFTP Server Username: {username}'.format(username=parameters['username_key']))
        Log.debug('SFTP Server Base Path: {base_path}'.format(base_path=parameters['path_key']))

        Log.debug('Creating EasySftpServer object...')
        return EasySftpServer(
            address=secret[source['address_key']],
            username=parameters['username_key'],
            password=parameters['password_key'],
            rsa_private_key=parameters['rsa_private_key_key'],
            fingerprint=parameters['fingerprint_key'],
            fingerprint_type=parameters['fingerprint_type_key'],
            port=parameters['port_key'],
            base_path=parameters['path_key'],
            log_level=Log.level
        )

    def __move_file__(self, source, destinations, file, base_path=''):
        """
        Move the specified file to the destinations

        :type source: EasySftpServer or EasyS3Bucket
        :param source: The file source

        :type destinations: list
        :param destinations: List of destinations to which the file should be moved

        :type base_path: str
        :param base_path: Optional base path to be prepended inside the destination folder (this is used for error files)

        :param file: File to move (one of the known file types)

        :return: None
        """
        Log.debug('Determining type of file to move...')

        if isinstance(file, EasyS3File):
            # Moving a file from AWS S3 bucket to the destination(s)
            Log.debug('Moving S3 file to specified destination(s)...')
            self.__move_s3_file__(
                source=source,
                destinations=destinations,
                file=file,
                base_path=base_path
            )
        elif isinstance(file, str) and os.path.exists(self.__current_file__):
            # Moving a file from local filesystem to the destination(s)
            Log.debug('Moving local file to specified destination(s)...')
            self.__move_local_file__(
                source=source,
                destinations=destinations,
                file=file,
                base_path=base_path
            )
        else:
            # Unknown file type
            Log.error('Unknown file type specified in move request: {type}'.format(type=type(file)))
            raise Exception('Unknown file type referenced during file move')

    def __move_s3_file__(self, source, destinations, file, base_path=''):
        """
        Move file from S3 to the destinations

        :type destinations: list
        :param destinations: List of destinations to which the file should be moved

        :type file: EasyS3File
        :param file: S3 file to move

        :type base_path: str
        :param base_path: Optional base path to be prepended inside the destination folder (this is used for error files)

        :return: None
        """
        error_move = False
        error_type = False

        Log.debug('Moving S3 file to destination(s)...')
        Log.debug('Source Bucket Name: {source_bucket_name}'.format(source_bucket_name=file.get_bucket_name()))
        Log.debug('Source Bucket Filename: {source_bucket_filename}'.format(source_bucket_filename=file.get_bucket_filename()))

        # Iterate through all destinations we need to move this file to
        for destination in destinations:
            try:
                if error_move is True:
                    # If the move failed to a previous destination, skip this file for the remainder of destinations
                    Log.error('Failed to move file to a previous destination, skipping all remaining destinations...')
                    break

                destination_filename = self.__easy_s3__.sanitize_path('{path}/{filename}'.format(
                    path=destination.get_base_path(),
                    filename=file.get_bucket_filename()[len(source.get_base_path()):]
                ))

                if len(base_path) > 0:
                    Log.debug('Destination Path Offset: {base_path}'.format(base_path=base_path))
                    destination_filename = self.__easy_s3__.sanitize_path('{base_path}/{destination_filename}'.format(
                        base_path=base_path,
                        destination_filename=destination_filename
                    ))

                if isinstance(destination, EasyS3Bucket):
                    # Moving an S3 to another S3 file
                    Log.debug('Copying file to S3 bucket...')

                    Log.debug('Destination Bucket Name: {destination_bucket_name}'.format(destination_bucket_name=destination.get_bucket_name()))
                    Log.debug('Destination Base Path: {destination_base_path}'.format(destination_base_path=destination.get_base_path()))
                    Log.debug('Destination Filename: {destination_filename}'.format(destination_filename=destination_filename))

                    file.copy(
                        destination_bucket_name=destination.get_bucket_name(),
                        destination_filename=destination_filename
                    )

                    Log.debug('Copied to destination S3 bucket')
                elif isinstance(destination, EasySftpServer):
                    # Uploading an S3 to an SFTP server
                    Log.debug('Uploading file to SFTP server...')
                    Log.debug('Destination Filename: {destination_filename}'.format(destination_filename=destination_filename))

                    local_filename = self.__easy_s3__.sanitize_path('{temp_folder}/{filename}'.format(
                        temp_folder=self.__download_path__,
                        filename=file.get_bucket_filename()
                    ))

                    Log.debug('Downloading file content from AWS S3 to local filesystem...')
                    file.download(local_filename)
                    Log.debug('Downloaded: {local_filename}'.format(local_filename=local_filename))

                    # Upload it to the SFTP server
                    Log.debug('Uploading file to SFTP server...')
                    destination.upload(
                        local_filename=local_filename,
                        remote_filename=destination_filename
                    )
                    Log.debug('Uploaded to destination SFTP server')
                else:
                    # Unknown destination type, log an error and set error flag
                    Log.error('Unknown destination type specified: {type}'.format(type=type(destination)))
                    error_type = True

            except Exception as move_exception:
                Log.error(move_exception)
                error_move = True

        if error_move is True:
            Log.error('File failed to move to one or more destination(s)')

        # If any move failed, raise an Exception
        if error_type is True:
            Log.error('One or more files was of an unknown type')

        if error_type is True or error_move is True:
            raise Exception('An unexpected exception error occurred while moving file to one or more destination(s), please review error logs for more information')

        # If there was no exception, delete the source file
        try:
            Log.debug('Deleting source file...')
            file.delete()
        except Exception as delete_exception:
            Log.error(delete_exception)
            Log.error('Exception error while attempting to delete original source file, the file may remain in the source folder, manual intervention may be required')

    def __move_local_file__(self, source, destinations, file, base_path=''):
        """
        Move file from local filesystem to the destinations

        :type destinations: list
        :param destinations: List of destinations to which the file should be moved

        :type source:
        :param source: Unused parameter

        :type file: str
        :param file: Filename/path to be moved

        :type base_path: str
        :param base_path: Optional base path to be prepended inside the destination folder (this is used for error files)

        :return: None
        """
        error_move = False
        error_type = False

        for destination in destinations:
            try:
                if isinstance(destination, EasyS3Bucket):
                    # Uploading a local file to an S3 file
                    Log.debug('Uploading local file to S3 bucket...')

                    destination_filename = self.__easy_s3__.sanitize_path('{base_path}/{path}'.format(
                        base_path=destination.get_base_path(),
                        # Strip temporary download path from filename
                        path=file[len(self.__download_path__):]
                    ))

                    if len(base_path) > 0:
                        Log.debug('Destination Path Offset: {base_path}'.format(base_path=base_path))
                        destination_filename = self.__easy_s3__.sanitize_path('{base_path}/{destination_filename}'.format(
                            base_path=base_path,
                            destination_filename=destination_filename
                        ))

                    Log.debug('Destination Bucket Name: {destination_bucket_name}'.format(destination_bucket_name=destination.get_bucket_name()))
                    Log.debug('Destination Base Path: {destination_base_path}'.format(destination_base_path=destination.get_base_path()))
                    Log.debug('Source Filename: {source_filename}'.format(source_filename=file))
                    Log.debug('Destination Filename: {destination_filename}'.format(destination_filename=destination_filename))

                    destination.upload_file(
                        upload_filename=file,
                        bucket_filename=destination_filename
                    )
                elif isinstance(destination, EasySftpServer):
                    # Uploading a local file to an SFTP server
                    destination_filename = file[len(self.__download_path__):]

                    if len(base_path) > 0:
                        Log.debug('Destination Path Offset: {base_path}'.format(base_path=base_path))
                        destination_filename = self.__easy_s3__.sanitize_path('{base_path}/{destination_filename}'.format(
                            base_path=base_path,
                            destination_filename=destination_filename
                        ))

                    Log.debug('Uploading local file to SFTP server...')
                    Log.debug('Destination Filename: {destination_filename}'.format(destination_filename=destination_filename))

                    destination.upload(
                        local_filename=file,
                        remote_filename=destination_filename
                    )
                    Log.debug('Uploaded to destination SFTP server')
                else:
                    # Unknown destination type, log an error and set error flag
                    Log.error('Unknown destination type specified: {type}'.format(type=type(destination)))
                    error_type = True

            except Exception as move_exception:
                Log.error(move_exception)
                error_move = True

        if error_move is True:
            Log.error('File failed to move to one or more destination(s)')

        if error_type is True:
            Log.error('One or more files was of an unknown type')

        if error_type is True or error_move is True:
            raise Exception('An unexpected exception error occurred while moving file to one or more destination(s), please review error logs for more information')

        # If there was no exception, delete the source file
        try:
            Log.debug('Deleting source file...')
            os.unlink(file)
        except Exception as delete_exception:
            Log.error(delete_exception)
            Log.error('Exception error while attempting to delete original source file, the file may remain in the source folder, manual intervention may be required')

    def __stake_sftp_server__(self, sftp_server, maximum_files=None) -> list:
        """
        Stake files from an SFTP server

        :type sftp_server: EasySftpServer
        :param sftp_server: The SFTP server to connect to

        :type maximum_files: int/None
        :param maximum_files: Maximum number of files to iterate

        :return: list of str
        """
        staked_files = []

        # Connect to SFTP server
        sftp_server.connect()

        # Download all files
        sftp_server.download_recursive(
            remote_path='/',
            local_path=self.__download_path__,
            maximum_files=maximum_files
        )

        # Disconnect from the server
        sftp_server.disconnect()

        # Get filenames we downloaded
        for root, directories, files in os.walk(self.__download_path__):
            for file in files:
                current_filename = '{root}/{file}'.format(root=root, file=file)
                staked_files.append(current_filename)

        # Return list of filenames
        return staked_files

    def __stake_s3_bucket__(self, s3_bucket, maximum_files=None) -> list:
        """
        Stake files in S3 bucket

        :type s3_bucket: EasyS3Bucket
        :param s3_bucket: The S3 bucket from which to stake files

        :type maximum_files: int/None
        :param maximum_files: Maximum number of files to iterate

        :return: list of EasyS3File
        """
        Log.debug('Attempting to stake files in S3 bucket...')
        Log.debug('Bucket Name: {bucket_name}'.format(bucket_name=s3_bucket.get_bucket_name()))
        Log.debug('Bucket Base Path: {bucket_base_path}'.format(bucket_base_path=s3_bucket.get_base_path()))

        staked_files = []
        count_files = 0

        Log.debug('Listing files in bucket...')
        files = s3_bucket.list_files()

        if files is not None:
            count_files_found = len(files)
            Log.debug('Found {count} files in bucket'.format(count=count_files_found))
        else:
            Log.debug('No files were found in bucket')

        # Iterate over each of the files
        for filename in files.keys():
            Log.debug('Attempting to stake file: {filename}'.format(filename=filename))
            easy_s3_file = files[filename]

            if easy_s3_file.stake() is True:
                Log.debug('Successfully staked: {filename}'.format(filename=filename))
                staked_files.append(easy_s3_file)
                count_files += 1
                if count_files >= maximum_files:
                    Log.debug('Reached maximum number of files, terminating staking process early...')
                    break
            else:
                Log.debug('Failed to stake file: {filename}'.format(filename=filename))

        Log.debug('Successfully staked {count} file(s)...'.format(count=len(staked_files)))

        # Return array of staked S3 files
        return staked_files
