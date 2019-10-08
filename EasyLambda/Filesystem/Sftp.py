#!/usr/bin/env python
# -*- coding: utf-8 -*-

from EasyLambda.Filesystem.BaseFilesystem import BaseFilesystem
from EasyLambda.Filesystem.File import File
from EasyLambda.EasyHelpers import EasyHelpers
from EasyLambda.Iterator.Destination import Destination
from EasyLambda.Iterator.Source import Source
from EasyLambda.Filesystem.Stake import Stake
from EasyLambda.EasyLog import EasyLog
from EasyLambda.EasySftpServer import EasySftpServer


class Sftp(BaseFilesystem):
    def __init__(
            self,
            address,
            username,
            validate_fingerprint=True,
            password=None,
            rsa_private_key=None,
            fingerprint=None,
            fingerprint_type=None,
            port=22,
            base_path='',
            host_key_checking=True,
    ):
        """
        Instantiate SFTP filesystem

        :type address: str
        :param address: Host server sftp_address/IP sftp_address

        :type port: int
        :param port: SFTP sftp_port number

        :type username: str
        :param username: Username for authentication

        :type rsa_private_key: str
        :param rsa_private_key: Private key for authentication

        :type password: str
        :param password: Password for authentication

        :type fingerprint: str
        :param fingerprint: Host sftp_fingerprint

        :type fingerprint_type: str
        :param fingerprint_type: Host sftp_fingerprint type

        :type validate_fingerprint: bool
        :param validate_fingerprint: Flag indicating SFTP server fingerprint should be validated

        :type base_path: str
        :param base_path: Base SFTP file path, all uploads/downloads will have this path prepended

        :type host_key_checking: bool
        :param host_key_checking: Flag indicating whether host key  will be validated on connection
        """
        EasyLog.trace('Instantiating SFTP filesystem...')

        self.__address__ = address
        self.__port__ = port
        self.__username__ = username
        self.__rsa_private_key__ = rsa_private_key
        self.__password__ = password
        self.__fingerprint__ = fingerprint
        self.__fingerprint_type__ = fingerprint_type
        self.__base_path__ = EasyHelpers.sanitize_path(base_path)
        self.__host_key_checking__ = host_key_checking

        self.__sftp_server__ = EasySftpServer(
            address=address,
            username=username,
            password=password,
            rsa_private_key=rsa_private_key,
            fingerprint=fingerprint,
            fingerprint_type=fingerprint_type,
            validate_fingerprint=validate_fingerprint,
            port=port,
            base_path=base_path,
            host_key_checking=host_key_checking
        )

    def iterate_files(self, callback, maximum_files, success_destinations, failure_destinations, delete_on_success, delete_on_failure, recursive, staking_strategy) -> list:
        """
        Iterate all files in the filesystem and return the number of files that were iterated

        :type callback: function
        :param callback: User callback function that is executed for each iterated file

        :type maximum_files: int or None
        :param maximum_files: The maximum number of files to iterate

        :type success_destinations: list of Destination or None
        :param success_destinations: If defined, the destination filesystem where each files will be copied following their successful completion

        :type failure_destinations: list of EasyIteratorDestination or None
        :param failure_destinations: If defined, the destination filesystem where each files will be copied following their failure during iteration

        :type delete_on_success: bool
        :param delete_on_success: If True, files will be deleted from the source on successful iteration

        :type delete_on_failure: bool
        :param delete_on_failure: If True, files will be deleted from the source if an error occurs during iteration

        :type recursive: bool
        :param recursive: Flag indicating iteration should be performed recursively

        :type staking_strategy: str
        :param staking_strategy: The staking strategy to adopt

        :return: list[EasyIteratorStakedFile]
        """
        EasyLog.trace('Iterating files in SFTP filesystem...')

        filesystem_files = self.file_list(recursive=recursive)
        filesystem_files_staked = []

        EasyLog.debug('Iterator located {count_files} file(s)...'.format(count_files=len(filesystem_files)))

        for filesystem_file in filesystem_files:
            try:
                # Stake the file using the appropriate method
                try:
                    EasyLog.debug('Staking: {filename}...'.format(filename=filesystem_file.get_filename()))
                    filesystem_file_staked = self.stake_file(filesystem_file, staking_strategy)
                    filesystem_files_staked.append(filesystem_file_staked)
                except Exception as staking_exception:
                    # Continue on to next file if we couldn't stake the current file- this is not a fatal error unless it keeps occurring (which will need to be monitored with an alarm)
                    EasyLog.warning('Staking Failed: {staking_exception}'.format(staking_exception=staking_exception))
                    continue

                # If the file was successfully staked, trigger the user callback
                EasyLog.debug('Triggering user callback...')
                try:
                    iteration_success = callback(staked_file=filesystem_file_staked)
                except Exception as callback_exception:
                    EasyLog.exception('The user callback function generated an unexpected exception error', callback_exception)
                    raise callback_exception

                if iteration_success is True and success_destinations is not None:
                    # The user callback succeeded, move to success destinations
                    EasyLog.debug('Moving iterated file to success destination(s)...')
                    Sftp.copy_staked_file_to_destinations(
                        filesystem_file=filesystem_file_staked,
                        destinations=success_destinations
                    )
                elif failure_destinations is not None:
                    # The user callback failed- move to failure destinations
                    EasyLog.debug('Moving failed file to failure destination(s)...')
                    Sftp.copy_staked_file_to_destinations(
                        filesystem_file=filesystem_file_staked,
                        destinations=failure_destinations
                    )

            except Exception as iteration_failure:
                EasyLog.exception(BaseFilesystem.ERROR_ITERATION_FAILURE, iteration_failure)
                # Delete the file is that flag was set
                if delete_on_failure is True:
                    EasyLog.debug('Deleting failed file from source location...')
                    pass

        return filesystem_files_staked

    @staticmethod
    def copy_staked_file_to_destinations(filesystem_file, destinations) -> None:
        """
        Move a staked file to the specified destination(s)

        :type filesystem_file: File
        :param filesystem_file: The staked file to move

        :type destinations: list[EasyIteratorDestination
        :param destinations: One or more iterator destination to which the file should be copied

        :return: None
        """
        # If there are no destinations, get out of here`
        if destinations is None or len(destinations) == 0:
            EasyLog.debug('No destinations defined')
            return

        # Otherwise copy the file to each of the destinations
        for destination in destinations:
            EasyLog.debug('Copying file to destination...')

            if isinstance(destination, Destination) is False:
                raise Exception('Unknown destination filesystem specified')

            destination.file_upload(
                local_filename=filesystem_file.get_stake().get_staked_filename(),
                destination_filename=filesystem_file.get_filename()
            )

    def stake_file(self, filesystem_file, staking_strategy) -> File:
        """
        Attempt to obtain an exclusive lock on a file from an SFTP server

        :type filesystem_file: File
        :param filesystem_file: The file to be staked

        :type staking_strategy: str
        :param staking_strategy: The staking strategy to adopt (currently only EasyIteratorSource.STRATEGY_IGNORE)

        :return: FilesystemFile
        """
        if staking_strategy == Source.STRATEGY_IGNORE:
            EasyLog.debug('Staking Strategy: Ignore')
            filesystem_file = self.__stake_file_ignore_strategy__(filesystem_file=filesystem_file)
        elif staking_strategy == Source.STRATEGY_RENAME:
            EasyLog.error('Staking Strategy: Rename File')
            raise Exception(Source.ERROR_STAKING_STRATEGY_UNSUPPORTED)
        elif staking_strategy == Source.STRATEGY_PROPERTY:
            EasyLog.error('Staking Strategy: Update File Property')
            raise Exception(Source.ERROR_STAKING_STRATEGY_UNSUPPORTED)
        else:
            # Unknown staking strategy specified
            EasyLog.error(Source.ERROR_STAKING_STRATEGY_INVALID)
            raise Exception(Source.ERROR_STAKING_STRATEGY_INVALID)

        return filesystem_file

    def __stake_file_ignore_strategy__(self, filesystem_file) -> File:
        """
        Download the file local, and ignore the requirement for staking to be exclusive
        
        :type filesystem_file: File
        :param filesystem_file: The file to be staked
        
        :return: FilesystemFile
        """
        EasyLog.trace('Staking file with ignore strategy...')

        # Generate unique local filename
        local_temp_folder = EasyHelpers.create_unique_local_temp_path()
        local_filename = EasyHelpers.sanitize_path('{local_temp_folder}/{path}'.format(
            local_temp_folder=local_temp_folder,
            path=filesystem_file.get_filename()
        ))

        # Download the file locally
        EasyLog.debug('Downloading: {path}'.format(path=filesystem_file.get_filename()))
        self.__get_sftp_server__().file_download(
            local_filename=local_filename,
            remote_filename=filesystem_file.get_filename()
        )

        # Create file stake object
        file_stake = Stake(
            filesystem=self,
            staking_strategy=Source.STRATEGY_IGNORE,
            staked_filename=local_filename
        )

        # Update the file with the stake
        filesystem_file.set_stake(file_stake)

        # Return the updated file
        return filesystem_file

    def file_list(self, path='', recursive=False) -> list:
        """
        List files in the filesystem path

        :type path: str
        :param path: Path in the filesystem to list (relative to whatever base path may be defined)

        :type recursive: bool
        :param recursive: Flag indicating the listing should be recursive. If False, sub-folder contents will not be returned

        :return: list[FilesystemFile]
        """
        filenames = self.__get_sftp_server__().file_list(
            remote_path=path,
            recursive=recursive
        )

        filesystem_files = []

        for filename in filenames:
            filesystem_files.append(File(
                filesystem=self,
                filename=filename
            ))

        return filesystem_files

    def file_exists(self, filename) -> bool:
        """
        Check if file exists in the filesystem

        :type filename: str
        :param filename: The name of the file to check in the filesystem (relative to whatever base path may be defined)

        :return: bool
        """
        return self.__get_sftp_server__().file_exists(
            remote_filename=filename
        )

    def file_delete(self, filename) -> None:
        """
        Delete a file from the filesystem

        :type filename: str
        :param filename: The name of the file to be deleted from the filesystem (relative to whatever base path may be defined)

        :return: None
        """
        self.__get_sftp_server__().file_delete(
            remote_filename=filename
        )

    def file_download(self, filename, local_filename):
        """
        Download a file from the filesystem to local storage

        :type filename: str
        :param filename: The name of the file in the filesystem to download (relative to whatever base path may be defined)

        :type local_filename: str
        :param local_filename: The destination on the local filesystem where the file will be downloaded to

        :return:
        """
        self.__get_sftp_server__().file_download(
            remote_filename=filename,
            local_filename=local_filename
        )

    def file_upload(self, filename, local_filename):
        """
        Upload a file from local storage to the filesystem

        :type filename: str
        :param filename: The destination on the filesystem where the file will be uploaded to  (relative to whatever base path may be defined)

        :type local_filename: str
        :param local_filename: The name of the local filesystem file that will be uploaded

        :return: None
        """
        self.__get_sftp_server__().file_upload(
            remote_filename=filename,
            local_filename=local_filename
        )

    def __get_sftp_server__(self) -> EasySftpServer:
        """
        Ensure the SFTP server is connected and return the server object
        
        :return: EasySftpServer
        """
        EasyLog.trace('Checking connection status...')

        if self.__sftp_server__.is_connected() is False:
            EasyLog.debug('Not connected, opening connection...')
            self.__sftp_server__.connect()

        return self.__sftp_server__
