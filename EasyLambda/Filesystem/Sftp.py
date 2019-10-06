from EasyLambda.EasySftpServer import EasySftpServer
from EasyLambda.EasyIterator import EasyIterator
from EasyLambda.EasyLog import EasyLog
from EasyLambda.EasyValidator import EasyValidator

# noinspection PyBroadException
from EasyLambda.Filesystem.File import File
from EasyLambda.Iterator.Destination import Destination


class Sftp(EasyLog):
    # Error constants
    ERROR_INVALID_SFTP_CONFIGURATION = 'The supplied SFTP sources configuration was invalid'
    ERROR_USER_CALLBACK = 'An unexpected error occurred while executing the user callback function'
    ERROR_MOVE_SUCCESS = 'An unexpected error occurred while moving the current file to the success destination'
    ERROR_MOVE_ERROR = 'An unexpected error occurred while moving the current file to the error destination'

    # Cache staked files
    staked_files = {}

    def __init__(self, aws_event, aws_context, easy_aws, configuration):
        """
        Configure SFTP filesystem object

        This class should not be instantiated directly, please use the IteratorFilesystemFactory class

        :type aws_event: dict
        :param aws_event: AWS Lambda uses this parameter to pass in event data to the handler

        :type aws_context: LambdaContext
        :param aws_context: AWS Lambda uses this parameter to provide runtime information to your handler

        :type easy_aws: EasyAws
        :param easy_aws: EasyAws object used by this class

        :type configuration: dict
        :param configuration: The filesystems configuration
        """
        EasyLog.trace('Initializing SFTP filesystem...')

        # Validate supplied configuration
        EasyLog.debug('Validating SFTP configuration...')
        ruleset = [
            # Mandatory fields
            'address',
            'port',
            'username',
            'base_path',
            # Either a password or an RSA key must be supplied
            {EasyValidator.RULE_ANY: ('password', 'rsa_private_key')},
            # The fingerprint and type must be supplied, or nothing must be supplied (in which case known hosts file will be used)
            {EasyValidator.RULE_ALL_OR_NOTHING: ('fingerprint', 'fingerprint_type')}
        ]

        if EasyValidator.validate(ruleset=ruleset, data=configuration) is False:
            EasyLog.error(Sftp.ERROR_INVALID_SFTP_CONFIGURATION)
            raise Exception(Sftp.ERROR_INVALID_SFTP_CONFIGURATION)

        EasyLog.debug('Validation completed successfully')

        # Create SFTP server object
        password = None
        if configuration['password'] is not None:
            EasyLog.debug('Using password authentication...')
            password = configuration['password']

        rsa_private_key = None
        if configuration['rsa_private_key'] is not None:
            EasyLog.debug('Using RSA private key authentication...')
            rsa_private_key = configuration['rsa_private_key']

        fingerprint = None
        if configuration['fingerprint'] is not None:
            EasyLog.debug('Adding host fingerprint...')
            fingerprint = configuration['fingerprint']

        fingerprint_type = None
        if configuration['fingerprint_type'] is not None:
            fingerprint_type = configuration['fingerprint_type']

        EasyLog.debug('Creating SFTP server object...')
        self.__easy_sftp_server__ = EasySftpServer(
            address=configuration['address'],
            port=int(configuration['port']),
            username=configuration['username'],
            base_path=configuration['base_path'],
            password=password,
            rsa_private_key=rsa_private_key,
            fingerprint=fingerprint,
            fingerprint_type=fingerprint_type
        )

    def iterate_files(
            self,
            callback,
            filesystems,
            maximum_files=None,
    ):
        """
        Iterate files from the current source

        :type callback: function
        :param callback: User callback function that is executed for each iterated file

        :type filesystems: dict
        :param filesystems: The iterators filesystems

        :type maximum_files: int or None
        :param maximum_files: The maximum number of files to iterate

        :return: list List of files that were iterated over
        """
        EasyLog.trace('Iterating files...')
        pass

    def stake(self, remote_filename) -> bool:
        """
        Attempt to claim exclusive ownership of a file in the remote filesystem

        :type remote_filename:
        :param remote_filename:

        :return: bool
        """
        EasyLog.trace('Staking file: {remote_filename}'.format(remote_filename=remote_filename))
        return self.__filesystem__.stake(
            remote_filename=remote_filename
        )

    def exists(self, remote_filename) -> bool:
        """
        Check if the specified remote filename exists

        :type remote_filename: str
        :param remote_filename: Remote path/filename to check

        :return: bool
        """
        EasyLog.trace('Checking exists: {remote_filename}'.format(remote_filename=remote_filename))

        return self.__easy_sftp_server__.exists(
            remote_filename=remote_filename
        )

    def list(self, remote_path, recursive=False) -> list:
        """
        List a list of all files accessible in the filesystem filesystem

        :type remote_path: str
        :param remote_path: The path in the filesystem to list

        :type recursive: bool
        :param recursive: If True the listing will proceed recursively down through all sub-folders

        :return: list
        """
        return self.__easy_sftp_server__.list(
            remote_path=remote_path,
            recursive=recursive
        )

    def delete(self, remote_filename) -> None:
        """
        Delete file from filesystem

        :type remote_filename: str
        :param remote_filename: Path/filename of remote file to be deleted

        :return: None
        """
        return self.__easy_sftp_server__.delete(
            remote_filename=remote_filename
        )

    def upload(self, local_filename, remote_filename) -> File:
        """
        Upload file from local filesystem to destination

        :type local_filename: str
        :param local_filename: Path/filename of local file to be uploaded

        :type remote_filename: str
        :param remote_filename: Destination path/filename in remote filesystem

        :return: File
        """
        self.__easy_sftp_server__.upload(
            local_filename=local_filename,
            remote_filename=remote_filename,
            confirm=False
        )

    def download(self, local_filename, remote_filename) -> File:
        """
        Download file from remote filesystem to local filesystem

        :type remote_filename: str
        :param remote_filename: Destination path/filename in local filesystem

        :type local_filename: str
        :param local_filename: Path/filename of local file to be uploaded

        :return: File
        """
        return self.__easy_sftp_server__.download(
            local_filename=local_filename,
            remote_filename=remote_filename
        )

    def iterate_files(
            self,
            callback,
            filesystems,
            maximum_files=None,
    ):
        """
        Iterate files from the current source

        :type callback: function
        :param callback: User callback function that is executed for each iterated file

        :type filesystems: dict
        :param filesystems: The iterators filesystems

        :type maximum_files: int or None
        :param maximum_files: The maximum number of files to iterate

        :return: list List of files that were iterated over
        """
        EasyLog.trace('Iterating SFTP filesystem...')

        # Stake files so that nobody else can claim them
        staked_files = self.__stake_files__(maximum_files=maximum_files)

        try:
            for file in staked_files:
                try:
                    EasyLog.debug('Triggering user callback...')
                    callback(file=file)
                    EasyLog.debug('User callback finished...')
                except Exception as callback_exception:
                    EasyLog.error(Sftp.ERROR_USER_CALLBACK)

                try:
                    for destination in filesystems[EasyIterator.FILESYSTEM_SUCCESS_DESTINATIONS]:
                        if isinstance(destination, Destination):
                            file.copy(
                                destination=destination,
                                destination_filename=file.get_filename(),
                                allow_overwrite=destination.is_overwrite_allowed()
                            )
                except Exception as move_success_exception:
                    EasyLog.exception('Unexpected error during move on success', move_success_exception)
                    raise Exception(Sftp.ERROR_MOVE_SUCCESS)

            # Check if there were success destinations defined
            if self.success_destinations is not None and len(self.success_destinations) > 0:
                # Copy file to success destinations
                EasyLog.trace('Copying to success destination(s)...')

                for destination in self.success_destinations:
                    print(destination)

                self.__copy_file__(
                    source=source,
                    destinations=self.success_destinations,
                    file=self.__current_file__
                )
            else:
                # No success destinations were defined, issue a warning
                EasyLog.warning('No success destination(s) were defined, files will remain in source folder...')

            # Now that we have finished, delete the file from the source if we've been asked to
            if self.get_sftp_server_flag(sftp_server=source, flag='delete') is True:
                # Connect to SFTP server and delete the file
                remote_filename = self.__current_file__[len(self.__download_path__):]
                EasyLog.debug('Deleting file from SFTP server: {remote_filename}'.format(remote_filename=remote_filename))

                EasyLog.trace('Connecting to SFTP server...')
                source.connect()

                EasyLog.trace('Checking if file exists...')
                if source.exists(remote_filename=self.__current_file__[len(self.__download_path__):]) is True:
                    EasyLog.trace('Deleting file...')
                    source.delete(remote_filename=self.__current_file__[len(self.__download_path__):])

                    EasyLog.trace('Disconnecting from SFTP server...')
                    source.disconnect()

                    EasyLog.trace('Delete completed successfully')
                else:
                    EasyLog.debug('File no longer exists, nothing to delete')
                    source.disconnect()
        except Exception as file_exception:
            error = True
            EasyLog.error(file_exception)
            EasyLog.error('An unexpected exception error during iteration of current file...')

            # Check if there were error destinations defined
            if self.error_destinations is not None and len(self.error_destinations) > 0:
                try:
                    EasyLog.error('Attempting to copy current file to error destination(s)...')
                    self.__copy_file__(
                        source=source,
                        destinations=self.error_destinations,
                        file=self.__current_file__,
                        base_path=self.start_time
                    )
                    EasyLog.error('Copy to error destinations completed successfully...')
                except Exception as cleanup_exception:
                    # The copy to the error destination failed, log an error message
                    EasyLog.error(cleanup_exception)
                    EasyLog.error('An unexpected exception error occurred during copy of files to error destination, failed file may remain in source folder...')

            else:
                # No error destinations were defined, issue a warning
                EasyLog.warning('No error destination(s) were defined, failed files will remain in source folder...')

except Exception as final_exception:
EasyLog.error(final_exception)
EasyLog.error('An unexpected exception error during iteration of current file...')

if error is True:
    EasyLog.error('One or more errors occurred while attempting to iterate the current file, please review the log messages for more information, manual intervention may be required...')

# Return files we attempted to iterate
return staked_files
