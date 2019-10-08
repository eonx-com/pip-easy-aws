#!/usr/bin/env python
# -*- coding: utf-8 -*-

import boto3
import filecmp
import os
import tempfile
import uuid

from EasyLog.Log import Log
from io import BytesIO


class Client:
    # Error constants
    ERROR_LOCAL_FILE_NOT_FOUND = 'The file was not found on the local filesystem'
    ERROR_LOCAL_FILE_UNREADABLE = 'The file was found on the local filesystem however its content were not readable, please check file permissions'
    ERROR_LIST_BUCKETS_EXCEPTION = 'An unexpected error occurred during listing of S3 buckets'
    ERROR_file_list_EXCEPTION = 'An unexpected error occurred during listing of files in S3 bucket'
    ERROR_FILE_EXISTS_EXCEPTION = 'An unexpected error occurred during test of file existence in S3 bucket'
    ERROR_FILE_DELETE_EXCEPTION = 'An unexpected error occurred while deleting S3 file'
    ERROR_FILE_DOWNLOAD_EXCEPTION = 'An unexpected error occurred while downloading file from S3'
    ERROR_FILE_UPLOAD_EXCEPTION = 'An unexpected error occurred while uploading file to S3'
    ERROR_FILE_GET_TAG_EXCEPTION = 'An unexpected error occurred while attempting to retrieve tags from file in S3'
    ERROR_FILE_PUT_TAG_EXCEPTION = 'An unexpected error occurred while attempting to update tags on file in S3'
    ERROR_FILE_COPY_EXCEPTION = 'An unexpected error occurred while copying  S3 file'
    ERROR_FILE_MOVE_EXCEPTION = 'An unexpected error occurred while moving S3 file'

    # Cache for CloudWatch client
    __client__ = None
    __uuid__ = None

    @staticmethod
    def get_s3_client():
        """
        Setup CloudWatch client
        """
        # If we haven't gotten a client yet- create one now and cache it for future calls
        if Client.__client__ is None:
            Log.trace('Instantiating AWS S3 client...')
            Client.__client__ = boto3.session.Session().client('s3')

        # Return the cached client
        return Client.__client__

    @staticmethod
    def get_uuid() -> str:
        """
        Retrieve unique UUID for the current instantiation of this class

        :return: str
        """
        # If we haven't use a UUID yet, generate one and cache it
        if Client.__uuid__ is None:
            Client.__uuid__ = uuid.uuid4()

        # Return the cached UUID
        return Client.__uuid__

    @staticmethod
    def list_buckets() -> list:
        """
        Return an array containing the names of all buckets accessible by the client

        :return: list
        """
        Log.trace('Listing S3 buckets...')
        buckets = []

        try:
            for bucket in Client.get_s3_client().list_buckets()['Buckets']:
                Log.debug('Found: {bucket}'.format(bucket=bucket['Name']))
                buckets.append(bucket['Name'])
        except Exception as list_exception:
            Log.exception(Client.ERROR_LIST_BUCKETS_EXCEPTION, list_exception)
            raise list_exception
        return buckets

    @staticmethod
    def file_list(bucket_name, bucket_path='', recursive=False) -> list:
        """
        List the content of a bucket/path

        :type bucket_name: string
        :param bucket_name: The bucket from which the objects are to be listed

        :type bucket_path: string
        :param bucket_path: The buckets path

        :type recursive: bool
        :param recursive: If true all sub-folder of the path will be iterated

        :return: list
        """
        Log.trace('Listing files in bucket...')
        bucket_path = Client.sanitize_path(bucket_path)

        recursive_enabled = 'No'
        if recursive is True:
            recursive_enabled = 'Yes'

        Log.debug('Bucket Name: {bucket_name}'.format(bucket_name=bucket_name))
        Log.debug('Bucket Path: {bucket_path}'.format(bucket_path=bucket_path))
        Log.debug('Listing Recursively: {recursive_enabled}'.format(recursive_enabled=recursive_enabled))

        # Get the content of the specified bucket
        try:
            files = []

            Log.debug('Retrieving file list...')
            objects = Client.get_s3_client().list_objects_v2(
                Bucket=bucket_name,
                Prefix=bucket_path,
                Delimiter='|'
            )

            while True:
                if 'Contents' in objects:
                    # Iterate through the content of the most recent search results
                    for object_current in objects['Contents']:
                        if 'Size' in object_current:
                            if object_current['Size'] > 0:
                                if 'Key' in object_current:
                                    filename = object_current['Key']

                                    # If we are not recursively searching, make it is in the specified path
                                    if recursive is False:
                                        path = os.path.dirname(filename)
                                        if path != bucket_path:
                                            Log.debug('Skipping file in path: {path}'.format(path=path))
                                            continue

                                            # Add the file to the list we will return
                                    Log.debug('Found: {filename}'.format(filename=filename))
                                    files.append(filename)

                # Check if the search results indicated there were more results
                if 'NextMarker' not in objects:
                    break

                # There were more results, rerun the search to get the next page of results
                Log.debug('Retrieving next page of results...')
                objects = Client.get_s3_client().list_objects_v2(
                    Bucket=bucket_name,
                    Delimiter='|',
                    NextMarker=objects['NextMarker']
                )
        except Exception as list_exception:
            Log.exception(Client.ERROR_file_list_EXCEPTION, list_exception)
            raise list_exception

        # Return files we found
        return files

    @staticmethod
    def file_exists(bucket_name, bucket_filename) -> bool:
        """
        Check if file exists in the specified bucket

        :type bucket_name: string
        :param bucket_name: Bucket to be searched

        :type bucket_filename: string
        :param bucket_filename: Path/filename to search for in bucket

        :return: bool
        """
        Log.trace('Checking S3 file exists...')
        Log.debug('Bucket Name: {bucket_name}'.format(bucket_name=bucket_name))
        Log.debug('Bucket Filename: {bucket_filename}'.format(bucket_filename=bucket_filename))

        try:
            # Retrieve list of files in the bucket
            files = Client.file_list(bucket_name=bucket_name, bucket_path=bucket_filename)

            result = bucket_filename in files

            if result is True:
                Log.debug('The requested file exists')
            else:
                Log.debug('The requested file could not be found')
        except Exception as exists_exception:
            Log.exception(Client.ERROR_FILE_EXISTS_EXCEPTION, exists_exception)
            raise exists_exception

        return result

    @staticmethod
    def file_delete(bucket_name, bucket_filename) -> bool:
        """
        Delete a file from S3 bucket

        :type bucket_name: string
        :param bucket_name: Bucket from which the file should be deleted

        :type bucket_filename: string
        :param bucket_filename: Path of the S3 file to be deleted

        :return: bool
        """
        Log.trace('Deleting file from S3 bucket...')
        Log.trace('Bucket Name: {bucket_name}'.format(bucket_name=bucket_name))
        Log.trace('Bucket Filename: {bucket_filename}'.format(bucket_filename=bucket_filename))

        try:
            # Make sure the file exists
            Log.trace('Checking if requested file exists...')
            if Client.file_exists(bucket_name=bucket_name, bucket_filename=bucket_filename) is False:
                raise Exception('The requested file did not exist, deleting failed')

            Log.trace('File found, deleting requested file...')
            result = Client.get_s3_client().delete_object(
                Bucket=bucket_name,
                Key=bucket_filename
            )
        except Exception as delete_exception:
            Log.exception(Client.ERROR_FILE_DELETE_EXCEPTION, delete_exception)
            raise delete_exception

        # Make sure the result is correctly formed and return boolean flag
        return 'DeleteMarker' in result

    @staticmethod
    def move_file(source_bucket_name, source_bucket_filename, destination_bucket_name, destination_bucket_filename) -> None:
        """
        Move a file in S3 to another bucket/path

        :type source_bucket_name: string
        :param source_bucket_name: The bucket the file should be moved from

        :type source_bucket_filename: string
        :param source_bucket_filename: The source filename

        :type destination_bucket_name: string
        :param destination_bucket_name: The bucket the file should be moved to

        :type destination_bucket_filename: string
        :param destination_bucket_filename: The destination filename

        :return: None
        """
        try:
            Log.trace('Moving S3 file...')
            Client.copy_file(
                source_bucket_name=source_bucket_name,
                source_bucket_filename=source_bucket_filename,
                destination_bucket_name=destination_bucket_name,
                destination_bucket_filename=destination_bucket_filename
            )
            Client.file_delete(
                bucket_name=source_bucket_name,
                bucket_filename=source_bucket_filename
            )
        except Exception as move_exception:
            Log.exception(Client.ERROR_FILE_MOVE_EXCEPTION, move_exception)
            raise move_exception

    @staticmethod
    def copy_file(source_bucket_name, source_bucket_filename, destination_bucket_name, destination_bucket_filename) -> None:
        """
        Copy a file in S3 to another bucket/path

        :type source_bucket_name: string
        :param source_bucket_name: The bucket the file should be copied from

        :type source_bucket_filename: string
        :param source_bucket_filename: The source filename

        :type destination_bucket_name: string
        :param destination_bucket_name: The bucket the file should be copied to

        :type destination_bucket_filename: string
        :param destination_bucket_filename: The destination filename

        :return: None
        """
        Log.trace('Copying S3 file...')

        # Sanitize supplied filenames
        source_bucket_filename = Client.sanitize_path(source_bucket_filename)
        destination_bucket_filename = Client.sanitize_path(destination_bucket_filename)

        Log.debug('Source Bucket Name: {source_bucket_name}'.format(source_bucket_name=source_bucket_name))
        Log.debug('Source Bucket Filename: {source_bucket_filename}'.format(source_bucket_filename=source_bucket_filename))
        Log.debug('Destination Bucket Name: {destination_bucket_name}'.format(destination_bucket_name=destination_bucket_name))
        Log.debug('Destination Bucket Filename: {destination_bucket_filename}'.format(destination_bucket_filename=destination_bucket_filename))

        # Perform some basic validation on the move

        if source_bucket_filename == destination_bucket_filename:
            raise Exception('The source and destination filenames cannot be the same')

        if Client.file_exists(bucket_name=source_bucket_name, bucket_filename=source_bucket_filename) is False:
            raise Exception('The requested file did not exist in the source bucket')

        if Client.file_exists(bucket_name=destination_bucket_name, bucket_filename=destination_bucket_filename) is False:
            raise Exception('The requested file did not exist in the destination bucket')

        try:
            # Copy the object to its destination
            Log.debug('Copying file to destination...')
            Client.get_s3_client().copy(
                Bucket=destination_bucket_name,
                CopySource={'Bucket': source_bucket_name, 'Key': source_bucket_filename},
                Key=destination_bucket_filename
            )
        except Exception as copy_exception:
            Log.exception('An unexpected error occurred while copying S3 file', copy_exception)
            raise copy_exception

    @staticmethod
    def file_download(bucket_name, bucket_filename, local_filename) -> None:
        """
        Download a file from an S3 bucket to local disk

        :type bucket_name: string
        :param bucket_name: Bucket from which the file should be downloaded

        :type bucket_filename: string
        :param bucket_filename: Path of the file to be downloaded in S3 bucket

        :type local_filename: str
        :param local_filename: Download filename on local filesystem

        :return: None
        """
        Log.trace('Downloading S3 file to local filesystem...')
        Log.debug('Bucket Name: {bucket_name}'.format(bucket_name=bucket_name))
        Log.debug('Bucket Filename: {bucket_filename}'.format(bucket_filename=bucket_filename))
        Log.debug('Destination: {local_filename}'.format(local_filename=local_filename))

        # Make sure the path exists in the local filesystem
        destination_path = os.path.dirname(local_filename)
        Log.debug('Checking if download location path exists...')
        if os.path.exists(destination_path) is False:
            Log.debug('Destination path does not exist, creating path...')
            os.makedirs(destination_path)

        try:
            Log.debug('Downloading...')
            Client.get_s3_client().file_download(
                Bucket=bucket_name,
                Key=bucket_filename,
                Filename=local_filename
            )
        except Exception as download_exception:
            Log.exception(Client.ERROR_FILE_DOWNLOAD_EXCEPTION, download_exception)
            raise download_exception

    @staticmethod
    def file_download_to_string(bucket_name, bucket_filename, encoding='utf-8') -> str:
        """
        Download a file from the bucket decoding it to a string

        :type bucket_name: string
        :param bucket_name: Bucket from which the file should be retrieved

        :type bucket_filename: string
        :param bucket_filename: Path of the S3 file to be retrieved

        :type encoding: string
        :param encoding: The files content_encoding, defaults to UTF-8

        :return: str
        """
        Log.trace('Downloading file from current bucket to string...')
        Log.trace('Bucket Name: {bucket_name}'.format(bucket_name=bucket_name))
        Log.trace('Bucket Filename: {bucket_filename}'.format(bucket_filename=bucket_filename))
        Log.trace('String Encoding: {encoding}'.format(encoding=encoding))

        try:
            return Client.get_s3_client().get_object(
                Bucket=bucket_name,
                Key=bucket_filename
            ).get('Body').read().decode(encoding)
        except Exception as download_exception:
            Log.exception(Client.ERROR_FILE_DOWNLOAD_EXCEPTION, download_exception)
            raise download_exception

    @staticmethod
    def file_upload(bucket_name, bucket_filename, local_filename) -> None:
        """
        Upload a local file to an S3 bucket

        :type bucket_name: string
        :param bucket_name: Bucket where file should be uploaded

        :type bucket_filename: string
        :param bucket_filename: Destination filename in S3 bucket

        :type local_filename: str
        :param local_filename: File on local filesystem to be uploaded

        :return: None
        """
        Log.trace('Uploading file from local filesystem to S3 bucket...')
        Log.debug('Uploading: {local_filename}'.format(local_filename=local_filename))
        Log.debug('Bucket Name: {bucket_name}'.format(bucket_name=bucket_name))
        Log.debug('Bucket Filename: {bucket_filename}'.format(bucket_filename=bucket_filename))

        # Make sure the local file exists
        Log.debug('Checking local file exists...')

        if os.path.exists(path=local_filename) is False:
            Log.error(Client.ERROR_LOCAL_FILE_NOT_FOUND)
            raise Exception(Client.ERROR_LOCAL_FILE_NOT_FOUND)

        # Make sure the file is readable

        Log.debug('Checking local file is readable...')

        local_file = open(local_filename, "r")
        local_file_readable = local_file.readable()
        local_file.close()

        if local_file_readable is False:
            Log.error(Client.ERROR_LOCAL_FILE_UNREADABLE)
            raise Exception(Client.ERROR_LOCAL_FILE_UNREADABLE)

        try:
            Log.debug('Uploading...')
            Client.get_s3_client().file_upload(
                Bucket=bucket_name,
                Key=bucket_filename,
                Filename=local_filename
            )
        except Exception as upload_exception:
            Log.exception(Client.ERROR_FILE_UPLOAD_EXCEPTION, upload_exception)
            raise upload_exception

    @staticmethod
    def file_upload_from_string(bucket_name, bucket_filename, contents, encoding='utf-8') -> None:
        """
        Upload the content of a string to the specified S3 bucket

        :type bucket_name: string
        :param bucket_name: Bucket where file should be uploaded

        :type bucket_filename: string
        :param bucket_filename: Destination filename in S3 bucket

        :type contents: string
        :param contents: The content to be uploaded to S3

        :type encoding: string
        :param encoding: The strings content_encoding, defaults to UTF-8

        :return: EasyS3File
        """
        Log.trace('Uploading string to S3 bucket...')
        Log.trace('Bucket Name: {bucket_name}'.format(bucket_name=bucket_name))
        Log.trace('Bucket Filename: {bucket_filename}'.format(bucket_filename=bucket_filename))
        Log.trace('String Encoding: {encoding}'.format(encoding=encoding))
        Log.trace('String Length: {string_length}'.format(string_length=len(contents)))

        try:
            Client.get_s3_client().file_uploadobj(
                Fileobj=BytesIO(bytes(contents, encoding)),
                Bucket=bucket_name,
                Key=bucket_filename
            )
        except Exception as upload_exception:
            Log.exception(Client.ERROR_FILE_UPLOAD_EXCEPTION, upload_exception)
            raise upload_exception

    @staticmethod
    def get_file_tags(bucket_name, bucket_filename) -> dict:
        """
        Return a list of tags on the specified file

        :type bucket_name: string
        :param bucket_name: Bucket in which the file is contained

        :type bucket_filename: string
        :param bucket_filename: Path of the S3 file

        :return: dict
        """
        Log.trace('Retrieving tags for S3 file...')
        Log.debug('Bucket Name: {bucket_name}'.format(bucket_name=bucket_name))
        Log.debug('Bucket Filename: {bucket_filename}'.format(bucket_filename=bucket_filename))

        try:
            object_tags = Client.get_s3_client().get_object_tagging(
                Bucket=bucket_name,
                Key=bucket_filename
            )

            keys = object_tags.keys()
            returned_tags = {}

            if 'TagSet' in keys:
                # Tags were found iterate through them and convert them into a dictionary of key/value pairs
                Log.debug('Found tags, iterating results...')

                for tag in object_tags['TagSet']:
                    Log.debug('Found: {tag_key}={tag_value}'.format(tag_key=tag['Key'], tag_value=tag['Value']))
                    returned_tags[tag['Key']] = tag['Value']
            else:
                # No tags were found
                Log.debug('No tags were found for specified file')

        except Exception as get_tag_exception:
            Log.exception(Client.ERROR_FILE_GET_TAG_EXCEPTION, get_tag_exception)
            raise get_tag_exception

        return returned_tags

    @staticmethod
    def set_file_tags(bucket_name, bucket_filename, tags) -> None:
        """
        Replace all tags on a file with those specified

        :type bucket_name: str
        :param bucket_name: Name of the bucket

        :type bucket_filename: str
        :param bucket_filename: Name of the file

        :type tags: dict
        :param tags: Dictionary of tags to set on the file, this will overwrite all existing tags

        :return: None
        """
        Log.trace('Setting AWS S3 file tags...')
        Log.debug('Bucket Name: {bucket_name}'.format(bucket_name=bucket_name))
        Log.debug('Bucket Filename: {bucket_filename}'.format(bucket_filename=bucket_filename))

        try:
            # Create list object to pass to S3 with key/value pairs
            tag_set = []
            for key in tags.keys():
                Log.debug('Tag: {key}={value}'.format(key=key, value=tags[key]))
                tag_set.append({'Key': key, 'Value': tags[key]})

            Log.debug('Writing tags to S3 object...')
            Client.get_s3_client().put_object_tagging(
                Bucket=bucket_name,
                Key=bucket_filename,
                Tagging={'TagSet': tag_set}
            )
        except Exception as put_tag_exception:
            Log.exception(Client.ERROR_FILE_PUT_TAG_EXCEPTION, put_tag_exception)
            raise put_tag_exception

    @staticmethod
    def test_path_permissions(bucket_name, bucket_path='') -> bool:
        """
        Create a test file with known content, upload it to S3, download it again, and compare the content

        :type bucket_name: str
        :param bucket_name: Bucket to test

        :type bucket_path: str
        :param bucket_path: Path in bucket to create test file

        :return: bool
        """
        Log.trace('Testing S3 path permissions...')
        Log.debug('Bucket Name: {bucket_name}'.format(bucket_name=bucket_name))
        Log.debug('Bucket Path: {bucket_path}'.format(bucket_path=bucket_path))

        # Generate test filenames
        bucket_filename = Client.sanitize_path('{path}/{bucket}.{uuid}.test.txt'.format(
            path=bucket_path,
            bucket=bucket_name,
            uuid=Client.get_uuid()
        ))
        original_filename = Client.sanitize_path('{temp_dir}/{bucket}.{uuid}.test.txt'.format(
            temp_dir=tempfile.gettempdir(),
            bucket=bucket_name,
            uuid=Client.get_uuid()
        ))
        downloaded_filename = '{original_filename}.downloaded'.format(original_filename=original_filename)

        # Make sure destination path on local filesystem exists
        destination_path = os.path.dirname(original_filename)

        Log.debug('Checking if destination path exists...')

        if os.path.exists(destination_path) is False:
            Log.debug('Destination path does not exist, creating destination path: {destination_path}'.format(destination_path=destination_path))
            os.makedirs(destination_path)

        # Create test file
        Log.debug('Creating local test file: {original_filename}...'.format(original_filename=original_filename))
        test_contents = ''
        for i in range(100):
            test_contents = str(uuid.uuid4())

        test_file = open(original_filename, "w")
        test_file.write(test_contents)
        Log.debug('Closing file object')
        test_file.close()

        # Display file size
        Log.debug('Test file size: {size} Bytes'.format(size=len(test_contents)))

        # Make that mother trucking file dance
        Log.debug('Uploading test file to S3 bucket...')
        Client.file_upload(
            bucket_name=bucket_name,
            bucket_filename=bucket_filename,
            local_filename=original_filename
        )

        Log.debug('Downloading test file from S3 bucket: downloaded_filename...'.format(downloaded_filename=downloaded_filename))
        Client.file_download(
            bucket_name=bucket_name,
            bucket_filename=bucket_filename,
            local_filename=downloaded_filename
        )

        # Compare the two files
        Log.debug('Comparing downloaded file to original test file...')
        test_result = filecmp.cmp(original_filename, downloaded_filename)

        # Delete the files locally
        Log.debug('Deleting local test file...')
        os.unlink(original_filename)

        Log.debug('Deleting downloaded test file...')
        os.unlink(downloaded_filename)

        Log.debug('Deleting uploaded file...')
        Client.file_delete(
            bucket_name=bucket_name,
            bucket_filename=bucket_filename
        )

        # Return the test result
        if test_result is True:
            Log.debug('File content matched')
        else:
            Log.error('File content did not match during S3 permission test')

        return test_result

    @staticmethod
    def sanitize_path(path) -> str:
        """
        In addition to the standard sanitization of paths, S3 filenames should not start with a slash- otherwise the file will be placed into an unnamed folder

        :type path: str
        :param path: Path to sanitize

        :return: str
        """
        path = str(path).strip()

        while '//' in path:
            path = path.replace('//', '/')

        # Strip all leading slashes
        while path.startswith('/'):
            path = path[1:]

        return path
