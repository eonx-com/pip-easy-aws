from EasySftp.EasySftpServer import EasySftpServer

from EasyLambda.EasyValidator import EasyValidator


class SftpFilesystem(EasyValidator):
    # Error constants
    ERROR_INVALID_SFTP_CONFIGURATION = 'The supplied SFTP sources configuration was invalid'

    def __init__(self, configuration):
        """
        Configure SFTP filesystem object

        This class should not be instantiated directly, please use the IteratorFilesystemFactory class

        :type configuration: dict
        :param configuration: The filesystems configuration
        """
        # Initialize parameter validation class
        EasyValidator.__init__(self=self)

        # Validate supplied configuration
        requirements = (
            # Mandatory fields
            'address',
            'port',
            'username',
            'base_path',
            # Either a password or an RSA key must be supplied
            {EasyValidator.RULE_OR: ('password', 'rsa_private_key')},
            # The fingerprint and type must be supplied, or nothing must be supplied (in which case known hosts file will be used)
            {EasyValidator.RULE_ALL_OR_NOTHING: ('fingerprint', 'fingerprint_type')}
        )
        if EasyValidator.validate_parameters(requirements=requirements, parameters=configuration) is False:
            raise Exception(SftpFilesystem.ERROR_INVALID_SFTP_CONFIGURATION)

        # Create SFTP server object
        password = None
        if configuration['password'] is not None:
            password = configuration['password']

        rsa_private_key = None
        if configuration['rsa_private_key'] is not None:
            rsa_private_key = configuration['rsa_private_key']

        fingerprint = None
        if configuration['fingerprint'] is not None:
            fingerprint = configuration['fingerprint']

        fingerprint_type = None
        if configuration['fingerprint_type'] is not None:
            fingerprint_type = configuration['fingerprint_type']

        self.__easy_sftp_server__ = EasySftpServer(
            address=configuration['address'],
            port=configuration['port'],
            username=configuration['username'],
            base_path=configuration['base_path'],
            password=password,
            rsa_private_key=rsa_private_key,
            fingerprint=fingerprint,
            fingerprint_type=fingerprint_type
        )

    def iterate_files(self, callback, maximum_files):
        """
        Iterate files from the SFTP server

        :param callback: function
        :param callback: Callback function to execute

        :type maximum_files: int
        :param maximum_files: Maximum number of files to iterate

        :return: list
        """
        self.log_trace('Iterating files from SFTP server...')

        if maximum_files is None:
            # No file limit
            self.log_trace('No file limit set, staking all available files...')
            staked_files = self.__stake_sftp_server__(source)
        else:
            # Ensure we don't download more than the maximum files (less the number already downloaded)
            remaining_files = maximum_files - count_files
            self.log_trace('File limit set, maximum of {remaining_files} file(s) remaining...'.format(remaining_files=remaining_files))
            staked_files = self.__stake_sftp_server__(source, maximum_files=remaining_files)

        error = False

        try:
            for local_filename in staked_files:
                self.__current_file__ = local_filename

                if os.path.exists(self.__current_file__) is False:
                    self.log_error('The current file could not be located on the local filesystem, skipping...')
                    continue

                try:
                    self.log_trace('Current filename: {current_file}'.format(current_file=self.__current_file__))

                    # Trigger the users callback for each staked file
                    self.log_trace('Triggering user callback function...')
                    callback(
                        file=self.__current_file__,
                        base_path=self.__download_path__,
                        easy_iterator=self
                    )

                    self.log_trace('User callback function completed...')

                    # Check if there were success destinations defined
                    if self.success_destinations is not None and len(self.success_destinations) > 0:
                        # Copy file to success destinations
                        self.log_trace('Copying to success destination(s)...')

                        for destination in self.success_destinations:
                            print(destination)

                        self.__copy_file__(
                            source=source,
                            destinations=self.success_destinations,
                            file=self.__current_file__
                        )
                    else:
                        # No success destinations were defined, issue a warning
                        self.log_warning('No success destination(s) were defined, files will remain in source folder...')

                    # Now that we have finished, delete the file from the source if we've been asked to
                    if self.get_sftp_server_flag(sftp_server=source, flag='delete') is True:
                        # Connect to SFTP server and delete the file
                        remote_filename = self.__current_file__[len(self.__download_path__):]
                        self.log_debug('Deleting file from SFTP server: {remote_filename}'.format(remote_filename=remote_filename))

                        self.log_trace('Connecting to SFTP server...')
                        source.connect()

                        self.log_trace('Checking if file exists...')
                        if source.exists(remote_filename=self.__current_file__[len(self.__download_path__):]) is True:
                            self.log_trace('Deleting file...')
                            source.delete(remote_filename=self.__current_file__[len(self.__download_path__):])

                            self.log_trace('Disconnecting from SFTP server...')
                            source.disconnect()

                            self.log_trace('Delete completed successfully')
                        else:
                            self.log_debug('File no longer exists, nothing to delete')
                            source.disconnect()
                except Exception as file_exception:
                    error = True
                    self.log_error(file_exception)
                    self.log_error('An unexpected exception error during iteration of current file...')

                    # Check if there were error destinations defined
                    if self.error_destinations is not None and len(self.error_destinations) > 0:
                        try:
                            self.log_error('Attempting to copy current file to error destination(s)...')
                            self.__copy_file__(
                                source=source,
                                destinations=self.error_destinations,
                                file=self.__current_file__,
                                base_path=self.start_time
                            )
                            self.log_error('Copy to error destinations completed successfully...')
                        except Exception as cleanup_exception:
                            # The copy to the error destination failed, log an error message
                            self.log_error(cleanup_exception)
                            self.log_error('An unexpected exception error occurred during copy of files to error destination, failed file may remain in source folder...')

                    else:
                        # No error destinations were defined, issue a warning
                        self.log_warning('No error destination(s) were defined, failed files will remain in source folder...')

        except Exception as final_exception:
            self.log_error(final_exception)
            self.log_error('An unexpected exception error during iteration of current file...')

        if error is True:
            self.log_error('One or more errors occurred while attempting to iterate the current file, please review the log messages for more information, manual intervention may be required...')

        # Return files we attempted to iterate
        return staked_files
