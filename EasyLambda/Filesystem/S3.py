from EasyLambda.EasyLog import EasyLog
from EasyLambda.EasyValidator import EasyValidator


class S3(EasyLog):
    # Error constants
    ERROR_INVALID_S3_CONFIGURATION = 'The supplied S3 sources configuration was invalid'

    def __init__(self, aws_context, aws_event, easy_aws, configuration):
        """
        Configure S3 filesystem object

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
        # Initialize logging class
        EasyLog.__init__(
            self=self,
            aws_event=aws_event,
            aws_context=aws_context,
            easy_aws=self.easy_aws
        )

        # Validate supplied configuration
        requirements = (
            # Mandatory fields
            'bucket_name',
            'base_path'
        )
        if EasyValidator.validate_parameters(rules=requirements, data=configuration) is False:
            raise Exception(S3.ERROR_INVALID_S3_CONFIGURATION)

        # Get S3 client
        self.__s3_client__ = easy_aws.get_s3_client()

        # Get S3 bucket object
        self.__s3_bucket__ = easy_aws.get_s3_bucket_client(
            bucket_name=configuration['bucket_name'],
            base_path=configuration['base_path']
        )

    def iterate_files(self, callback, maximum_files):
        """
        Iterate files from the S3 bucket

        :param callback: function
        :param callback: Callback function to execute

        :type maximum_files: int
        :param maximum_files: Maximum number of files to iterate

        :return: list
        """
        self.log_trace('Iterating files from S3 bucket...')
        self.log_trace('Bucket Name: {bucket_name}'.format(bucket_name=self.__s3_bucket__.get_bucket_name()))
        self.log_trace('Base Path: {base_path}'.format(base_path=self.__s3_bucket__.get_base_path()))

        if maximum_files is None:
            # No file limit
            self.log_trace('No file limit set, staking all available files...')
            staked_files = self.__stake_s3_bucket__(source)
        else:
            # Ensure we don't download more than the maximum files (less the number already downloaded)
            remaining_files = maximum_files - count_files
            self.log_trace('File limit set, maximum of {remaining_files} file(s) remaining...'.format(remaining_files=remaining_files))
            staked_files = self.__stake_s3_bucket__(source, maximum_files=remaining_files)

        # Trigger the users callback for each staked file
        error = False

        try:
            for staked_file in staked_files:
                self.__current_file__ = staked_file

                try:
                    self.log_trace('Current filename: {current_file}'.format(current_file=self.__current_file__.get_bucket_filename()))

                    if isinstance(staked_file, EasyS3File):
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
                            self.__copy_file__(
                                source=source,
                                destinations=self.success_destinations,
                                file=self.__current_file__
                            )
                        else:
                            # No success destinations were defined, issue a warning
                            self.log_warning('No success destination(s) were defined, files will remain in source folder...')

                        # If we need to delete the file from its source, do so now...
                        if self.get_s3_bucket_flag(s3_bucket=source, flag='delete') is True:
                            remote_filename = self.__current_file__.get_bucket_filename()
                            self.log_debug('Deleting file from S3 server: {remote_filename}'.format(remote_filename=remote_filename))

                            try:
                                self.log_trace('Checking if file exists...')
                                if self.__s3_bucket__.file_exists(bucket_filename=self.__current_file__.get_bucket_filename()) is True:
                                    self.log_trace('Deleting file...')
                                    self.__current_file__.delete()
                                    self.log_trace('Delete complete')
                                else:
                                    self.log_trace('File no longer exists, nothing to delete')

                            except Exception as delete_exception:
                                self.log_error(delete_exception)
                                self.log_warning('Failed to delete file from source, please review logs- manual intervention may be required')

                            self.log_trace('Delete complete')

                    else:
                        self.log_error('Unexpected file type while iterating S3 bucket...')
                        continue

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
                            self.log_error('An unexpected exception error occurred during copying of files to error destination, failed file may remain in source folder...')

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

    def __create_easy_s3_bucket__(self, source) -> EasyS3Bucket:
        """
        Create an EasyS3Bucket object from the Lambda functions input variables

        :type source: dict
        :param source: The input parameters details

        :return: EasyS3Bucket
        """
        # Make sure the file source contains required S3 bucket name and path
        error = False

        for parameter in ('bucket_name', 'base_path'):
            if parameter not in source:
                error = True
                self.log_error('The supplied S3 source does not contain required "{parameter}" value, please check your function configuration'.format(
                    parameter=parameter
                ))

        if error is True:
            raise Exception('One or more required parameters was not set correctly')

        self.log_debug('Adding S3 bucket: {bucket_name}/{base_path}'.format(
            bucket_name=source['bucket_name'],
            base_path=source['base_path']
        ))

        s3_bucket = self.__easy_s3__.get_s3_bucket_client(
            bucket_name=source['bucket_name'],
            base_path=source['base_path'],
        )

        return s3_bucket
