from EasyLambda.EasyFilesystemDriver import EasyFilesystemDriver
from EasyLambda.EasyHelpers import EasyHelpers
from EasyLambda.EasyIteratorDestination import EasyIteratorDestination
from EasyLambda.EasyIteratorSource import EasyIteratorSource
from EasyLambda.EasyLog import EasyLog
from EasyLambda.EasySftp import EasySftp
from EasyLambda.EasySftpServer import EasySftpServer


# noinspection PyBroadException
class EasyFilesystemDriverSftp(EasyFilesystemDriver):
    def __init__(
            self,
            address,
            username,
            password=None,
            rsa_private_key=None,
            fingerprint=None,
            fingerprint_type=None,
            port=22,
            base_path='',
            host_key_checking=True,
    ):
        """
        Instantiate SFTP filesystem driver

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

        :type base_path: str
        :param base_path: Base SFTP file path, all uploads/downloads will have this path prepended

        :type host_key_checking: bool
        :param host_key_checking: Flag indicating whether host key  will be validated on connection
        """
        EasyLog.trace('Instantiating SFTP filesystem driver...')

        self.__address__ = address
        self.__port__ = port
        self.__username__ = username
        self.__rsa_private_key__ = rsa_private_key
        self.__password__ = password
        self.__fingerprint__ = fingerprint
        self.__fingerprint_type__ = fingerprint_type
        self.__base_path__ = base_path
        self.__host_key_checking__ = host_key_checking

        self.__sftp_server__ = EasySftpServer(
            address=address,
            username=username,
            password=password,
            rsa_private_key=rsa_private_key,
            fingerprint=fingerprint,
            fingerprint_type=fingerprint_type,
            port=port,
            base_path=base_path,
            host_key_checking=host_key_checking
        )

    def iterate_files(self, callback, maximum_files, success_destinations, failure_destinations, delete_on_success, delete_on_failure, recursive, staking_strategy) -> int:
        """
        Iterate all files in the filesystem and return the number of files that were iterated

        :type callback: function
        :param callback: User callback function that is executed for each iterated file

        :type maximum_files: int or None
        :param maximum_files: The maximum number of files to iterate

        :type success_destinations: list of EasyIteratorDestination or None
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

        :return: int Number of files iterated
        """
        EasyLog.trace('Iterating files in SFTP filesystem...')

        EasyLog.debug('Checking connection status...')
        if self.__sftp_server__.is_connected() is False:
            EasyLog.debug('Not connected, opening connection...')
            self.__sftp_server__.connect()

        files_iterated = 0
        filenames = self.__sftp_server__.file_list(
            remote_path='/',
            recursive=recursive
        )

        for filename in filenames:
            try:
                # Stake the file using the appropriate method
                try:
                    staked_filename = self.stake_file(filename, staking_strategy)
                    EasyLog.debug('Staked: {staked_filename}'.format(staked_filename=staked_filename))
                except Exception:
                    # Continue on to next file if we couldn't stake the current file
                    EasyLog.warning('Unable to stake file: {filename}'.format(filename=filename))
                    continue

                # If the file was successfully staked, trigger the user callback
                EasyLog.debug('Triggering user callback...')
                try:
                    iteration_success = callback(staked_filename=staked_filename)
                except Exception as callback_exception:
                    EasyLog.exception('The user callback function generated an unexpected exception error', callback_exception)
                    raise callback_exception

                # Increase iteration count
                files_iterated += 1

                if iteration_success is True and success_destinations is not None:
                    # The user callback succeeded, move to success destinations
                    EasyLog.debug('Moving iterated file to success destination(s)...')
                    self.move_staked_file_to_destinations(
                        original_filename=filename,
                        staked_filename=staked_filename,
                        destinations=success_destinations
                    )
                elif failure_destinations is not None:
                    # The user callback failed- move to failure destinations
                    EasyLog.debug('Moving failed file to failure destination(s)...')
                    self.move_staked_file_to_destinations(
                        original_filename=filename,
                        staked_filename=staked_filename,
                        destinations=failure_destinations
                    )

            except Exception as iteration_failure:
                EasyLog.exception(EasyFilesystemDriver.ERROR_ITERATION_FAILURE, iteration_failure)
                # Delete the file is that flag was set
                if delete_on_failure is True:
                    EasyLog.debug('Deleting failed file from source location...')
                    pass

        return files_iterated

    def move_staked_file_to_destinations(self, original_filename, staked_filename, destinations) -> None:
        """
        Move a staked file to the specified destination(s)

        :type staked_filename: str
        :param staked_filename: The name of the local copy of the staked file

        :type destinations: list[EasyIteratorDestination]
        :param destinations: EasyIteratorDestination

        :return: None
        """
        # If there are no destinations, get out of here
        if destinations is None or len(destinations) == 0:
            EasyLog.debug('No destinations defined')
            return

        # Otherwise copy the file to each of the destinations
        for destination in destinations:
            EasyLog.debug('Copying file to destination...')

            if isinstance(destination, EasyIteratorDestination) is False:
                raise Exception('Unknown destination filesystem specified')

            destination.file_upload(
                local_filename=staked_filename,
                destination_filename=original_filename
            )

    def stake_file(self, filesystem_path, staking_strategy) -> str:
        """
        Attempt to obtain an exclusive lock on a file from an SFTP server

        :type filesystem_path: str
        :param filesystem_path: The path/file of the file to stake from the SFTP server

        :type staking_strategy: str
        :param staking_strategy: The staking strategy to adopt (currently only EasyIteratorSource.STRATEGY_IGNORE)

        :return: str The filename on local filesystem
        """

        if staking_strategy == EasyIteratorSource.STRATEGY_IGNORE:
            # Staking is assumed to have worked if the download succeeded (i.e. Direct Link)
            EasyLog.debug('Ignoring staking, all downloaded files are considered successfully staked...')
            # Download the file locally
            # Generate unique local filename
            local_temp_folder = EasyHelpers.create_unique_local_temp_path()
            local_filename = EasySftp.sanitize_path('{local_temp_folder}/{filesystem_path}'.format(
                local_temp_folder=local_temp_folder,
                filesystem_path=filesystem_path
            ))

            EasyLog.debug('Downloading file: {filesystem_path}'.format(filesystem_path=filesystem_path))
            self.__sftp_server__.file_download(
                local_filename=local_filename,
                remote_filename=filesystem_path
            )
            return local_filename
        elif staking_strategy == EasyIteratorSource.STRATEGY_RENAME:
            # Add a unique suffix to the filename on the SFTP server, then make sure we were the first process to rename it by checking
            # it exists with our unique suffix
            EasyLog.debug('Using renaming staking strategy...')
            EasyLog.error('This strategy has not yet been implemented')
            raise Exception(EasyIteratorSource.ERROR_STAKING_STRATEGY_UNSUPPORTED)
        elif staking_strategy == EasyIteratorSource.STRATEGY_PROPERTY:
            # Add a unique property to the file- there is no way to do this with SFTP
            EasyLog.error('The SFTP filesystem does not support property based staking')
            raise Exception(EasyIteratorSource.ERROR_STAKING_STRATEGY_UNSUPPORTED)
        else:
            raise Exception(EasyIteratorSource.ERROR_STAKING_STRATEGY_INVALID)

    def file_list(self, filesystem_path, recursive=False) -> list:
        """
        List files in the filesystem path

        :type filesystem_path: str
        :param filesystem_path: Path in the filesystem to list (relative to whatever base path may be defined)

        :type recursive: bool
        :param recursive: Flag indicating the listing should be recursive. If False, sub-folder contents will not be returned

        :return: list
        """
        return self.__sftp_server__.file_list(
            remote_path=filesystem_path,
            recursive=recursive
        )

    def file_exists(self, filesystem_filename) -> bool:
        """
        Check if file exists in the filesystem

        :type filesystem_filename: str
        :param filesystem_filename: The name of the file to check in the filesystem (relative to whatever base path may be defined)

        :return: bool
        """
        return self.__sftp_server__.file_exists(
            remote_filename=filesystem_filename
        )

    def file_delete(self, filesystem_filename) -> None:
        """
        Delete a file from the filesystem

        :type filesystem_filename: str
        :param filesystem_filename: The name of the file to be deleted from the filesystem (relative to whatever base path may be defined)

        :return: None
        """
        self.__sftp_server__.file_delete(
            remote_filename=filesystem_filename
        )

    def file_download(self, filesystem_filename, local_filename):
        """
        Download a file from the filesystem to local storage

        :type filesystem_filename: str
        :param filesystem_filename: The name of the file in the filesystem to download (relative to whatever base path may be defined)

        :type local_filename: str
        :param local_filename: The destination on the local filesystem where the file will be downloaded to

        :return:
        """
        self.__sftp_server__.file_download(
            remote_filename=filesystem_filename,
            local_filename=local_filename
        )

    def file_upload(self, filesystem_filename, local_filename):
        """
        Upload a file from local storage to the filesystem

        :type filesystem_filename: str
        :param filesystem_filename: The destination on the filesystem where the file will be uploaded to  (relative to whatever base path may be defined)

        :type local_filename: str
        :param local_filename: The name of the local filesystem file that will be uploaded

        :return: None
        """
        self.__sftp_server__.file_upload(
            remote_filename=filesystem_filename,
            local_filename=local_filename
        )
