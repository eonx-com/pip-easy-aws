#!/usr/bin/env python
# -*- coding: utf-8 -*-

import boto3
import filecmp
import os
import tempfile
import uuid

from EasyLog.Log import Log
from io import BytesIO

# noinspection PyBroadException
from EasyS3.ClientError import ClientError


class Client:
    # Cache for S3 client
    __client__ = None
    __uuid__ = None

    @staticmethod
    def get_s3_client():
        """
        Retrieve S3 client
        """
        # If we haven't gotten a client yet- create one and cache it for future calls
        if Client.__client__ is None:
            Log.trace('Instantiating S3 Client...')
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
            Log.debug('Generated S3 Client UUID: {uuid}'.format(uuid=Client.__uuid__))

        # Return the cached UUID
        return Client.__uuid__

    @staticmethod
    def list_buckets() -> list:
        """
        Return an array containing the names of all buckets accessible by the client

        :return: list[str]
        """
        buckets = []
        list_buckets_result = []

        try:
            Log.trace('Listing S3 Buckets...')
            list_buckets_result = Client.get_s3_client().list_buckets()
        except Exception as list_exception:
            # Failed to retrieve bucket list
            Log.exception(ClientError.ERROR_BUCKET_LIST_UNHANDLED_EXCEPTION, list_exception)

        # Make sure the result contains the expected key
        Log.debug('Checking Result...')
        if 'Buckets' not in list_buckets_result:
            # Missing expected buckets key in response from S3
            Log.exception(ClientError.ERROR_BUCKET_LIST_KEY_MISSING)

        # Dump all files found for debugging purposes
        if len(list_buckets_result) == 0:
            Log.debug('No Buckets Found')
        else:
            Log.debug('Buckets Found: ')
            for bucket in list_buckets_result:
                # Make sure the name key exists in the result
                if 'Name' in bucket is False:
                    Log.exception(ClientError.ERROR_BUCKET_LIST_KEY_MISSING)

                Log.debug('- {bucket}'.format(bucket=bucket['Name']))
                buckets.append(bucket['Name'])

        # Return the bucket names
        Log.debug('Bucket List Completed')
        return buckets

    @staticmethod
    def file_list(bucket_name, bucket_path='', recursive=False) -> list:
        """
        List the contents of the specified bucket/path

        :type bucket_name: string
        :param bucket_name: The bucket from which the objects are to be listed

        :type bucket_path: string
        :param bucket_path: The buckets path

        :type recursive: bool
        :param recursive: If true all sub-folder of the path will be iterated

        :return: list[str]
        """
        files = []

        # Sanitize the bucket path
        bucket_path = Client.sanitize_path(bucket_path)

        Log.trace('Listing Files...')
        Log.debug('Bucket Name: {bucket_name}'.format(bucket_name=bucket_name))
        Log.debug('Bucket Path: {bucket_path}'.format(bucket_path=bucket_path))
        Log.debug('Listing Recursively: {recursive}'.format(recursive=('Yes', 'No')[recursive]))

        try:
            # Retrieve list of files
            list_objects_result = Client.get_s3_client().list_objects_v2(Bucket=bucket_name, Prefix=bucket_path, Delimiter='|')

            while True:
                # Make sure the result contains the expected contents key
                if 'Contents' not in list_objects_result:
                    # The result did not contain the required key, throw an exception
                    Log.exception(ClientError.ERROR_FILE_LIST_KEY_NOT_FOUND)

                # Iterate through the content of the most recent search results
                for object_details in list_objects_result['Contents']:
                    # Make sure the result contains the expected filename key
                    if 'Key' not in object_details:
                        # The result did not contain the required key, throw an exception
                        Log.exception(ClientError.ERROR_FILE_LIST_KEY_NOT_FOUND)

                    # Ignore anything with zero file size (as it is most likely a directory)
                    if 'Size' in object_details and object_details['Size'] > 0:
                        # Check if we are performing a recursive search
                        if recursive is False:
                            # We are not recursively searching, make the file is in the required path
                            if os.path.dirname(object_details['Key']) != bucket_path:
                                # Skip this file
                                continue

                        # Add the file to the list we will return
                        files.append(object_details['Key'])

                # Check if the search results indicated there were more results
                if 'NextMarker' not in list_objects_result:
                    break

                # There were more results, rerun the search to get the next page of results
                Log.debug('Loading Next Marker...')
                next_marker = list_objects_result['NextMarker']
                list_objects_result = Client.get_s3_client().list_objects_v2(Bucket=bucket_name, Delimiter='|', NextMarker=next_marker)
        except Exception as list_exception:
            Log.exception(ClientError.ERROR_FILE_LIST_UNHANDLED_EXCEPTION, list_exception)

        # Dump all files for debugging
        if len(files) == 0:
            Log.debug('No Files Found')
        else:
            Log.debug('Files Found:')
            for filename in files:
                Log.debug('- {filename}'.format(filename=filename))

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
        file_list_result = []

        # Sanitize the bucket path
        bucket_filename = Client.sanitize_path(bucket_filename)

        Log.trace('Checking If File Exists...')
        Log.debug('Bucket Name: {bucket_name}'.format(bucket_name=bucket_name))
        Log.debug('Bucket Filename: {bucket_filename}'.format(bucket_filename=bucket_filename))

        try:
            # Retrieve list of files in the bucket
            Log.debug('Retrieving File List...')
            file_list_result = Client.file_list(bucket_name=bucket_name, bucket_path=bucket_filename)
        except Exception as exists_exception:
            Log.exception(ClientError.ERROR_FILE_EXISTS_UNHANDLED_EXCEPTION, exists_exception)

        # Check if the file exists
        Log.debug('Checking Existence...')
        file_exists_result = bucket_filename in file_list_result

        Log.debug('File {file_found}'.format(file_found=('Found', 'Not Found')[file_exists_result]))
        return file_exists_result

    @staticmethod
    def file_delete(bucket_name, bucket_filename) -> None:
        """
        Delete a file from S3 bucket

        :type bucket_name: string
        :param bucket_name: Bucket from which the file should be deleted

        :type bucket_filename: string
        :param bucket_filename: Path of the S3 file to be deleted

        :return: None
        """
        # Sanitize the bucket path
        bucket_filename = Client.sanitize_path(bucket_filename)

        Log.trace('Deleting File...')
        Log.debug('Bucket Name: {bucket_name}'.format(bucket_name=bucket_name))
        Log.debug('Bucket Filename: {bucket_filename}'.format(bucket_filename=bucket_filename))

        # Make sure the file exists before we try to delete it
        Log.debug('Checking File Exists Before Delete...')
        if Client.file_exists(bucket_name=bucket_name, bucket_filename=bucket_filename) is False:
            # The file to be deleted did not exist
            Log.exception(ClientError.ERROR_FILE_DELETE_SOURCE_NOT_FOUND)

        # Delete the file
        try:
            Log.debug('Deleting...')
            Client.get_s3_client().delete_object(Bucket=bucket_name, Key=bucket_filename)
        except Exception as delete_exception:
            # Unhandled exception during deletion
            Log.exception(ClientError.ERROR_FILE_DELETE_UNHANDLED_EXCEPTION, delete_exception)

        # Make sure the file no longer exists
        Log.debug('Checking Delete Result...')
        if Client.file_exists(bucket_name=bucket_name, bucket_filename=bucket_filename) is True:
            Log.exception(ClientError.ERROR_FILE_DELETE_FAILED)
        Log.debug('Delete successful...')

    @staticmethod
    def move_file(
            source_bucket_name,
            source_bucket_filename,
            destination_bucket_name,
            destination_bucket_filename,
            allow_overwrite=True
    ) -> None:
        """
        Move a file from one S3 bucket/path to another S3 bucket/path

        :type source_bucket_name: string
        :param source_bucket_name: The bucket the file should be moved from

        :type source_bucket_filename: string
        :param source_bucket_filename: The source filename

        :type destination_bucket_name: string
        :param destination_bucket_name: The bucket the file should be moved to

        :type destination_bucket_filename: string
        :param destination_bucket_filename: The destination filename

        :type allow_overwrite: bool
        :param allow_overwrite: Flag indicating the file is allowed to be overwritten if it already exists. If False, and the file exists an base_exception will be thrown

        :return: None
        """
        # Sanitize the bucket path
        destination_bucket_filename = Client.sanitize_path(destination_bucket_filename)

        Log.trace('Moving S3 File...')

        # Make sure the source file exists
        if Client.file_exists(bucket_name=source_bucket_name, bucket_filename=source_bucket_filename) is False:
            Log.exception(ClientError.ERROR_FILE_COPY_SOURCE_NOT_FOUND)

        # If we are not in overwrite mode, we need to check if the file exists already
        if allow_overwrite is False:
            Log.debug('Checking File Does Not Exist At Destination...')
            if Client.file_exists(bucket_name=destination_bucket_name, bucket_filename=destination_bucket_filename) is True:
                # The file already exists at the destination
                Log.exception(ClientError.ERROR_FILE_COPY_DESTINATION_EXISTS)

        # Copy the file
        try:
            Log.debug('Copying File To Destination...')
            Client.copy_file(
                source_bucket_name=source_bucket_name,
                source_bucket_filename=source_bucket_filename,
                destination_bucket_name=destination_bucket_name,
                destination_bucket_filename=destination_bucket_filename
            )
        except Exception as copy_exception:
            # Unexpected exception error occurred during copy operation
            Log.exception(ClientError.ERROR_FILE_MOVE_COPY_EXCEPTION, copy_exception)

        # Delete the file from the source bucket
        try:
            Log.debug('Deleting S3 File From Source...')
            Client.file_delete(bucket_name=source_bucket_name, bucket_filename=source_bucket_filename)
        except Exception as copy_exception:
            Log.exception(ClientError.ERROR_FILE_MOVE_DELETE_EXCEPTION, copy_exception)

        Log.debug('Move Completed')

    @staticmethod
    def copy_file(
            source_bucket_name,
            source_bucket_filename,
            destination_bucket_name,
            destination_bucket_filename,
            allow_overwrite=True
    ) -> None:
        """
        Copy S3 file to the specified destination

        :type source_bucket_name: string
        :param source_bucket_name: The bucket the file should be copied from

        :type source_bucket_filename: string
        :param source_bucket_filename: The source filename

        :type destination_bucket_name: string
        :param destination_bucket_name: The bucket the file should be copied to

        :type destination_bucket_filename: string
        :param destination_bucket_filename: The destination filename

        :type allow_overwrite: bool
        :param allow_overwrite: Flag indicating the file is allowed to be overwritten if it already exists. If False, and the file exists an base_exception will be thrown

        :return: None
        """
        # Sanitize the bucket path
        source_bucket_filename = Client.sanitize_path(source_bucket_filename)
        destination_bucket_filename = Client.sanitize_path(destination_bucket_filename)

        Log.trace('Copying File...')
        Log.debug('Source Bucket Name: {source_bucket_name}'.format(source_bucket_name=source_bucket_name))
        Log.debug('Source Bucket Filename: {source_bucket_filename}'.format(source_bucket_filename=source_bucket_filename))
        Log.debug('Destination Bucket Name: {destination_bucket_name}'.format(destination_bucket_name=destination_bucket_name))
        Log.debug('Destination Bucket Filename: {destination_bucket_filename}'.format(destination_bucket_filename=destination_bucket_filename))

        # Ensure the source and destination are not the same
        if source_bucket_filename == destination_bucket_filename:
            Log.exception(ClientError.ERROR_FILE_COPY_SOURCE_DESTINATION_SAME)

        # Make sure the source file exists
        if Client.file_exists(bucket_name=source_bucket_name, bucket_filename=source_bucket_filename) is False:
            Log.exception(ClientError.ERROR_FILE_COPY_SOURCE_NOT_FOUND)

        # If we are not in overwrite mode, we need to check if the file exists already
        if allow_overwrite is False:
            Log.debug('Checking File Does Not Exist...')
            if Client.file_exists(bucket_name=destination_bucket_name, bucket_filename=destination_bucket_filename) is True:
                # The file already exists at the destination
                Log.exception(ClientError.ERROR_FILE_COPY_DESTINATION_EXISTS)

        try:
            # Copy the file to its destination
            Log.debug('Copying File To Destination...')
            copy_source = {'Bucket': source_bucket_name, 'Key': source_bucket_filename}
            Client.get_s3_client().copy(CopySource=copy_source, Bucket=destination_bucket_name, Key=destination_bucket_filename)
        except Exception as copy_exception:
            # Something unexpected happened during the file copy
            Log.exception(ClientError.ERROR_FILE_COPY_UNHANDLED_EXCEPTION, copy_exception)

        # Make sure the file exists after the copy
        Log.debug('Checking File At Destination...')
        if Client.file_exists(bucket_name=source_bucket_name, bucket_filename=source_bucket_filename) is False:
            # File did not exist at the destination
            Log.exception(ClientError.ERROR_FILE_COPY_FAILED)

        Log.debug('Copy Completed')

    @staticmethod
    def file_download(
            bucket_name,
            bucket_filename,
            local_filename,
            allow_overwrite=True
    ) -> None:
        """
        Download a file from an S3 bucket to local disk

        :type bucket_name: string
        :param bucket_name: Bucket from which the file should be downloaded

        :type bucket_filename: string
        :param bucket_filename: Path of the file to be downloaded in S3 bucket

        :type local_filename: str
        :param local_filename: Download filename on local filesystem

        :type allow_overwrite: bool
        :param allow_overwrite: Flag indicating the file is allowed to be overwritten if it already exists. If False, and the file exists an base_exception will be thrown

        :return: None
        """
        # Sanitize the bucket path
        bucket_filename = Client.sanitize_path(bucket_filename)
        destination_path = os.path.dirname(local_filename)

        Log.trace('Downloading File To Disk...')
        Log.debug('Bucket Name: {bucket_name}'.format(bucket_name=bucket_name))
        Log.debug('Bucket Filename: {bucket_filename}'.format(bucket_filename=bucket_filename))
        Log.debug('Destination: {local_filename}'.format(local_filename=local_filename))

        # Make sure the file exists at the source
        Log.debug('Checking Source File Exists...')
        if Client.file_exists(bucket_name=bucket_name, bucket_filename=bucket_filename) is False:
            # Source file could not be found, copy cannot proceed
            Log.exception(ClientError.ERROR_FILE_DOWNLOAD_SOURCE_NOT_FOUND)

        if allow_overwrite is False:
            # If we are not in overwrite mode, we need to check if the file exists already
            Log.debug('Checking Destination File Does Not Exist...')
            if os.path.exists(local_filename) is True:
                Log.exception(ClientError.ERROR_FILE_DOWNLOAD_DESTINATION_EXISTS)

        # Make sure the destination path exists in the local filesystem
        Log.debug('Checking Download Path Exists...')
        if os.path.exists(destination_path) is False:
            # Download path didn't exist, we need to create it before download
            Log.debug('Creating Download Folder...')
            os.makedirs(destination_path)

        # Download the file
        try:
            Log.debug('Downloading...')
            Client.get_s3_client().download_file(Bucket=bucket_name, Key=bucket_filename, Filename=local_filename)
        except Exception as download_exception:
            # Unhandled exception during download
            Log.exception(ClientError.ERROR_FILE_DOWNLOAD_UNHANDLED_EXCEPTION, download_exception)

        # Make sure the file now exists locally
        Log.debug('Checking Downloaded File...')
        if os.path.exists(local_filename) is False:
            Log.exception(ClientError.ERROR_FILE_DOWNLOAD_DESTINATION_NOT_FOUND)

        # Make sure the file is readable via the load filesystem
        Log.debug('Checking Downloaded File Is Readable...')
        local_file = open(local_filename, "r")
        local_file_readable = local_file.readable()
        local_file.close()
        if local_file_readable is False:
            # The file could not be read after download
            Log.exception(ClientError.ERROR_FILE_DOWNLOAD_DESTINATION_NOT_READABLE)

        Log.debug('Download Completed')

    @staticmethod
    def file_download_to_string(
            bucket_name,
            bucket_filename,
            encoding='utf-8'
    ) -> str:
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
        # Sanitize the bucket path
        bucket_filename = Client.sanitize_path(bucket_filename)

        Log.trace('Downloading File To String...')
        Log.debug('Bucket Name: {bucket_name}'.format(bucket_name=bucket_name))
        Log.debug('Bucket Filename: {bucket_filename}'.format(bucket_filename=bucket_filename))
        Log.debug('String Encoding: {encoding}'.format(encoding=encoding))

        # Make sure we can find the file at the source before we start
        Log.debug('Checking Source File Exists...')
        if Client.file_exists(bucket_name=bucket_name, bucket_filename=bucket_filename) is False:
            # Source file could not be found, copy cannot proceed
            Log.exception(ClientError.ERROR_FILE_DOWNLOAD_SOURCE_NOT_FOUND)

        # Get the file object from S3
        file_object = None
        try:
            Log.debug('Retrieving S3 Object...')
            file_object = Client.get_s3_client().get_object(Bucket=bucket_name, Key=bucket_filename)
        except Exception as download_exception:
            # Something went wrong retrieving the object from S3
            Log.exception(ClientError.ERROR_FILE_DOWNLOAD_UNHANDLED_EXCEPTION, download_exception)

        # Retrieve the body of the object
        body = None
        try:
            Log.debug('Getting Object Body...')
            body = file_object.get('Body')
        except Exception as body_exception:
            # Error string decoding
            Log.exception(ClientError.ERROR_FILE_DOWNLOAD_BODY_EXCEPTION, body_exception)

        # Read the body into a variable
        body_content = None
        try:
            Log.debug('Reading Body...')
            body_content = body.read()
        except Exception as read_exception:
            # Error string decoding
            Log.exception(ClientError.ERROR_FILE_DOWNLOAD_READ_EXCEPTION, read_exception)

        # Perform string decoding
        body_string = None
        try:
            Log.debug('Decoding {encoding}...'.format(encoding=encoding.upper()))
            body_string = body_content.decode(encoding)
        except Exception as decode_exception:
            # Error string decoding
            Log.exception(ClientError.ERROR_FILE_DOWNLOAD_DECODE_EXCEPTION, decode_exception)

        Log.debug('Download Completed')
        return body_string

    @staticmethod
    def file_upload(
            bucket_name,
            bucket_filename,
            local_filename,
            allow_overwrite=True
    ) -> None:
        """
        Upload a local file to an S3 bucket

        :type bucket_name: string
        :param bucket_name: Bucket where file should be uploaded

        :type bucket_filename: string
        :param bucket_filename: Destination filename in S3 bucket

        :type local_filename: str
        :param local_filename: File on local filesystem to be uploaded

        :type allow_overwrite: bool
        :param allow_overwrite: Flag indicating the file is allowed to be overwritten if it already exists. If False, and the file exists an base_exception will be thrown

        :return: None
        """
        # Sanitize the bucket path
        bucket_filename = Client.sanitize_path(bucket_filename)

        Log.trace('Uploading file from local filesystem to S3 bucket...')
        Log.debug('Uploading: {local_filename}'.format(local_filename=local_filename))
        Log.debug('Bucket Name: {bucket_name}'.format(bucket_name=bucket_name))
        Log.debug('Bucket Filename: {bucket_filename}'.format(bucket_filename=bucket_filename))

        # Make sure the local file exists
        Log.debug('Checking local file exists...')
        if os.path.exists(path=local_filename) is False:
            Log.exception(ClientError.ERROR_FILE_UPLOAD_SOURCE_NOT_FOUND)

        # Make sure the file is readable
        Log.debug('Checking File Is Readable...')
        local_file = open(local_filename, "r")
        local_file_readable = local_file.readable()
        local_file.close()
        if local_file_readable is False:
            Log.error(ClientError.ERROR_FILE_UPLOAD_SOURCE_UNREADABLE)
            raise Exception(ClientError.ERROR_FILE_UPLOAD_SOURCE_UNREADABLE)

        # If we are not in overwrite mode, we need to check if the file exists already
        if allow_overwrite is False:
            if Client.file_exists(bucket_name=bucket_name, bucket_filename=bucket_filename) is True:
                Log.error(ClientError.ERROR_FILE_UPLOAD_DESTINATION_EXISTS)
                raise Exception(ClientError.ERROR_FILE_UPLOAD_DESTINATION_EXISTS)

        # Upload the file
        try:
            Log.debug('Uploading...')
            Client.get_s3_client().upload_file(
                Bucket=bucket_name,
                Key=bucket_filename,
                Filename=local_filename
            )
        except Exception as upload_exception:
            # An unexpected exception occurred during upload
            Log.exception(ClientError.ERROR_FILE_UPLOAD_UNHANDLED_EXCEPTION, upload_exception)
            raise upload_exception

        Log.debug('Upload Completed')

    @staticmethod
    def file_upload_from_string(bucket_name, bucket_filename, contents, encoding='utf-8', allow_overwrite=True) -> None:
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

        :type allow_overwrite: bool
        :param allow_overwrite: Flag indicating the file is allowed to be overwritten if it already exists. If False, and the file exists an base_exception will be thrown

        :return: EasyS3File
        """
        # Sanitize the bucket path
        bucket_filename = Client.sanitize_path(bucket_filename)

        Log.trace('Uploading string to S3 bucket...')
        Log.debug('Bucket Name: {bucket_name}'.format(bucket_name=bucket_name))
        Log.debug('Bucket Filename: {bucket_filename}'.format(bucket_filename=bucket_filename))
        Log.debug('String Encoding: {encoding}'.format(encoding=encoding))
        Log.debug('String Length: {string_length}'.format(string_length=len(contents)))

        if allow_overwrite is False:
            if Client.file_exists(
                    bucket_name=bucket_name,
                    bucket_filename=bucket_filename
            ) is True:
                Log.error(ClientError.ERROR_FILE_DOWNLOAD_DESTINATION_EXISTS)
                raise Exception(ClientError.ERROR_FILE_DOWNLOAD_DESTINATION_EXISTS)

        # Convert the string to a byte array
        byte_array = None
        try:
            byte_array = bytes(contents, encoding)
        except Exception as byte_array_exception:
            Log.exception(ClientError.ERROR_FILE_UPLOAD_SOURCE_TO_ARRAY_ERROR, byte_array_exception)

        # Convert the byte array to file like object
        file_object = None
        try:
            file_object = BytesIO(byte_array)
        except Exception as file_object_exception:
            Log.exception(ClientError.ERROR_FILE_UPLOAD_SOURCE_TO_FILE_OBJECT_ERROR, file_object_exception)

        # Upload the file object
        try:
            Log.debug('Uploading...')
            Client.get_s3_client().upload_fileobj(
                Fileobj=file_object,
                Bucket=bucket_name,
                Key=bucket_filename
            )
        except Exception as upload_exception:
            Log.exception(ClientError.ERROR_FILE_UPLOAD_UNHANDLED_EXCEPTION, upload_exception)
            raise upload_exception

        Log.debug('Upload Completed')

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
        # Sanitize the bucket path
        bucket_filename = Client.sanitize_path(bucket_filename)

        Log.trace('Retrieving tags for S3 file...')
        Log.debug('Bucket Name: {bucket_name}'.format(bucket_name=bucket_name))
        Log.debug('Bucket Filename: {bucket_filename}'.format(bucket_filename=bucket_filename))

        # Make sure the file exists before we try to do this
        Log.debug('Checking File Exists Before Reading Tags...')
        if Client.file_exists(bucket_name=bucket_name, bucket_filename=bucket_filename) is False:
            # The file to be deleted did not exist
            Log.exception(ClientError.ERROR_FILE_TAG_READ_SOURCE_NOT_FOUND)

        # Retrieve the existing tags
        keys = None
        object_tags = None
        try:
            Log.debug('Loading Tag Set...')
            object_tags = Client.get_s3_client().get_object_tagging(Bucket=bucket_name, Key=bucket_filename)
            keys = object_tags.keys()
        except Exception as tag_exception:
            Log.exception(ClientError.ERROR_FILE_TAG_READ_EXCEPTION, tag_exception)

        # Check if any tags existed
        if 'TagSet' not in keys:
            # No tags found, return the empty dictionary
            Log.debug('No Tags Found')
            return {}

        # Iterate tags into a dictionary
        Log.debug('Tags Found')
        tags = {}
        for tag in object_tags['TagSet']:
            key = tag['key']
            value = tag['value']
            Log.debug('- {key}: {value}'.format(key=key, value=value))
            tags[key] = value

        # Return the tags we found
        return tags

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
        # Sanitize the bucket path
        bucket_filename = Client.sanitize_path(bucket_filename)

        Log.trace('Setting AWS S3 file tags...')
        Log.debug('Bucket Name: {bucket_name}'.format(bucket_name=bucket_name))
        Log.debug('Bucket Filename: {bucket_filename}'.format(bucket_filename=bucket_filename))

        # Make sure the file exists before we try to do this
        Log.debug('Checking File Exists Before Setting Tags...')
        if Client.file_exists(bucket_name=bucket_name, bucket_filename=bucket_filename) is False:
            # The file to be deleted did not exist
            Log.exception(ClientError.ERROR_FILE_TAG_READ_SOURCE_NOT_FOUND)

        # Create list object to pass to S3 with key/value pairs
        tag_set = []
        for key in tags.keys():
            Log.debug('Tag: {key}={value}'.format(key=key, value=tags[key]))
            tag_set.append({'Key': key, 'Value': tags[key]})

        try:
            Log.debug('Writing tags to S3 object...')
            Client.get_s3_client().put_object_tagging(
                Bucket=bucket_name,
                Key=bucket_filename,
                Tagging={'TagSet': tag_set}
            )
        except Exception as put_tag_exception:
            Log.exception(ClientError.ERROR_FILE_TAG_WRITE_EXCEPTION, put_tag_exception)

        # Retrieve the tags were written correctly
        Log.debug('Checking Tags...')
        new_tags = Client.get_file_tags(bucket_name=bucket_name, bucket_filename=bucket_filename)

        # Validate the number of keys match
        if len(new_tags) != len(tags):
            Log.exception(ClientError.ERROR_FILE_TAG_WRITE_FAILED)

        # Validate each tag is present in the new values and the values match
        for key in tags.keys():
            if key not in new_tags:
                # The tag did not exist
                Log.exception(ClientError.ERROR_FILE_TAG_WRITE_FAILED)
            if tags[key] != new_tags[key]:
                # The values were not matched
                Log.exception(ClientError.ERROR_FILE_TAG_WRITE_FAILED)

    @staticmethod
    def test_client_path(bucket_name, bucket_path='') -> bool:
        """
        Create a test file with known contents, upload it to S3, download it again, and compare the content

        :type bucket_name: str
        :param bucket_name: Bucket to test

        :type bucket_path: str
        :param bucket_path: Path in bucket to create test file

        :return: bool
        """
        # Sanitize the bucket path
        bucket_path = Client.sanitize_path(bucket_path)

        Log.trace('Testing S3 path permissions...')
        Log.debug('Bucket Name: {bucket_name}'.format(bucket_name=bucket_name))
        Log.debug('Bucket Path: {bucket_path}'.format(bucket_path=bucket_path))

        # Generate test filenames
        bucket_filename = Client.sanitize_path('{path}/{bucket}.{uuid}.test.txt'.format(path=bucket_path, bucket=bucket_name, uuid=Client.get_uuid()))
        original_filename = Client.sanitize_path('{temp_dir}/{bucket}.{uuid}.test.txt'.format(temp_dir=tempfile.gettempdir(), bucket=bucket_name, uuid=Client.get_uuid()))
        downloaded_filename = '{original_filename}.downloaded'.format(original_filename=original_filename)

        # Make sure destination path on local filesystem exists
        Log.debug('Ensuring Destination Path Exists...')
        destination_path = os.path.dirname(original_filename)
        if os.path.exists(destination_path) is False:
            Log.debug('Creating Destination Path...')
            os.makedirs(destination_path)

        # Create test file
        Log.debug('Creating Test File Locally')
        test_contents = ''
        for i in range(100):
            test_contents = str(uuid.uuid4())

        test_file = open(original_filename, "w")
        test_file.write(test_contents)
        Log.debug('Closing file object')
        test_file.close()

        # Upload the tet files
        Log.debug('Uploading Test File...')
        Client.get_s3_client().upload_file(Bucket=bucket_name, Key=bucket_filename, Filename=original_filename)

        Log.debug('Downloading test file from S3 bucket: downloaded_filename...'.format(downloaded_filename=downloaded_filename))
        Client.get_s3_client().download_file(Bucket=bucket_name, Key=bucket_filename, Filename=downloaded_filename)

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
