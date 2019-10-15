#!/usr/bin/env python
# -*- coding: utf-8 -*-

import boto3
import os
import uuid

from EasyLocalDisk.Client import Client as LocalDiskClient
from EasyLog.Log import Log
from EasyFilesystem.S3.ClientError import ClientError


# noinspection DuplicatedCode
class Client:
    def __init__(self):
        """
        Setup S3 client
        """
        self.__boto3_s3_client__ = None

    @staticmethod
    def sanitize_path(path) -> str:
        """
        Sanitize a path

        :type path: str
        :param path: Path to be sanitized

        :return: str
        """
        # Remove all leading/trailing whitespace
        path = str(path).strip()

        # Remove all duplicated slashes in the path
        while '//' in path:
            path = path.replace('//', '/')

        # Strip all leading/trailing slashes
        path = path.strip('/')

        # Add slash to end of string
        path = '{path}/'.format(path=path)

        # If the path is root- then remove the slash
        if path == '/':
            path = ''

        return path

    @staticmethod
    def sanitize_filename(filename) -> str:
        """
        Sanitize a full path/filename

        :type filename: str
        :param filename: Path/Filename to sanitize

        :return: str
        """
        # Make sure we got a string
        filename = str(filename)

        # Sanitize the path component of the supplied string
        path = Client.sanitize_path(os.path.dirname(filename))

        # Extract the filename
        filename = os.path.basename(filename)

        # Concatenate them together
        return '{path}{filename}'.format(path=path, filename=filename)

    def create_path(self, bucket, path, allow_overwrite=False) -> None:
        """
        Create path in remote sftp_filesystem
        
        :type bucket:str
        :param bucket: The bucket in which the path will be created

        :type path: str
        :param path: The path to create

        :type allow_overwrite: bool
        :param allow_overwrite: Flag indicating the path is allowed to be overwritten if it already exists. If False, and the path exists an exception will be thrown

        :return: None
        """
        # Sanitize the supplied path
        path = self.sanitize_path(path)

        # If allow overwrite is disabled, make sure the path doesn't already exist
        if allow_overwrite is False:
            if self.file_exists(bucket=bucket, filename=path) is True:
                Log.exception(ClientError.ERROR_CREATE_PATH_ALREADY_EXISTS)

        # Create the path
        try:
            self.__get_boto3_s3_client__().put_object(Body='', Bucket=bucket, Key=path)
        except Exception as create_path_exception:
            Log.exception(ClientError.ERROR_CREATE_PATH_UNHANDLED_EXCEPTION, create_path_exception)

        # Make sure the path exists
        if self.path_exists(bucket=bucket, path=path) is False:
            Log.exception(ClientError.ERROR_CREATE_PATH_FAILED)

    def create_temp_path(self, bucket, prefix='', temp_path=None, auto_create=True) -> str:
        """
        Create a new temporary path that is guaranteed to be unique

        :type bucket:str
        :param bucket: The bucket in which the path will be created

        :type prefix: str
        :param prefix: Optional prefix to prepend to path

        :type temp_path: str
        :param temp_path: The base path for all temporary files

        :type auto_create: bool
        :param auto_create: If the temp path doesn't exist- automatically create it

        :return: str
        """
        # If not temp path is set, set a sensible default
        if temp_path is None:
            temp_path = '/temp/'

        # Sanitize the supplied path
        temp_path = self.sanitize_path(temp_path)

        # Make sure the temporary folder exists
        if self.file_exists(bucket=bucket, filename=temp_path) is False:
            # If auto create is enabled, create it- otherwise raise an exception
            if auto_create is False:
                Log.exception(ClientError.ERROR_CREATE_TEMP_PATH_FOLDER_NOT_FOUND)

            self.create_path(bucket=bucket, path=temp_path)

        # Enter a loop until we find a folder name that doesn't already exist
        count_attempts = 0
        while True:
            count_attempts += 1
            path = '{temp_path}{prefix}{uuid}'.format(temp_path=temp_path, prefix=prefix, uuid=uuid.uuid4())
            if self.path_exists(bucket=bucket, path=path) is False:
                # noinspection PyBroadException
                try:
                    self.create_path(bucket=bucket, path=path, allow_overwrite=False)
                    return path
                except Exception:
                    # If creation fails its likely because it exists- so we will just try again next time
                    pass
            if count_attempts > 10:
                # If we fail to create a path more than 10 times, something is wrong
                Log.exception(ClientError.ERROR_CREATE_TEMP_PATH_FAILED)

    def bucket_list(self) -> list:
        """
        Return list all buckets names that are accessible

        :return: List of bucket names
        """
        try:
            # Request list of buckets
            list_buckets_result = self.__get_boto3_s3_client__().bucket_list()
            if 'Buckets' not in list_buckets_result:
                Log.exception(ClientError.ERROR_BUCKET_LIST_INVALID_RESULT)

            # Get all of the bucket names into a list and return them
            buckets = []
            for bucket in list_buckets_result['Buckets']:
                if 'Name' not in bucket:
                    Log.exception(ClientError.ERROR_BUCKET_LIST_INVALID_RESULT)
                buckets.append(bucket['Name'])
            return buckets
        except Exception as list_exception:
            Log.exception(ClientError.ERROR_BUCKET_LIST_UNHANDLED_EXCEPTION, list_exception)

    def file_list(self, bucket, path, include_directories=False, recursive=False) -> list:
        """
        List the contents of the specified bucket/path

        :type bucket:str
        :param bucket: The bucket from which the objects are to be listed

        :type path:str
        :param path: The buckets path

        :type include_directories: bool
        :param include_directories: If true, directories will be included in the results

        :type recursive: bool
        :param recursive: If true all sub-folder of the path will be iterated

        :return: list[str]
        """
        # Sanitize the bucket path
        path = self.sanitize_path(path)

        files = []

        try:
            # Retrieve list of files
            list_objects_result = self.__get_boto3_s3_client__().list_objects_v2(Bucket=bucket, Prefix=path)

            while True:
                # Make sure the result contains the expected contents key
                if 'Contents' not in list_objects_result:
                    # The result did not contain any contents, get out of here
                    break

                # Iterate through the content of the most recent search results
                for object_details in list_objects_result['Contents']:
                    # Make sure the result contains the expected filename key
                    if 'Key' not in object_details:
                        # The result did not contain the required key, throw an exception
                        Log.exception(ClientError.ERROR_FILE_LIST_INVALID_RESULT)

                    # Check if we are performing a recursive search
                    if recursive is False:
                        # We are not recursively searching, make sure the file is in the required path
                        if Client.sanitize_path(os.path.dirname(object_details['Key'])) != path:
                            # Skip this file
                            continue

                    # Handle directories
                    if str(object_details['Key']).endswith('/') is True:
                        if include_directories is False:
                            continue

                    # Add the file to the list we will return
                    files.append(object_details['Key'])

                # Check if the search results indicated there were more results
                if 'NextMarker' not in list_objects_result:
                    break

                # There were more results, rerun the search to get the next page of results
                next_marker = list_objects_result['NextMarker']
                list_objects_result = self.__get_boto3_s3_client__().list_objects_v2(Bucket=bucket, NextMarker=next_marker)
        except Exception as list_exception:
            Log.exception(ClientError.ERROR_FILE_LIST_UNHANDLED_EXCEPTION, list_exception)

        # Return files we found
        return files

    def path_exists(self, bucket, path) -> bool:
        """
        Check if file exists in the specified bucket

        :type bucket:str
        :param bucket: Bucket to be searched

        :type path:str
        :param path: Path to search for in bucket

        :return: bool
        """
        # Sanitize the path
        path = Client.sanitize_path(path)

        file_list_result = []
        try:
            file_list_result = self.file_list(
                bucket=bucket,
                path=path,
                include_directories=True
            )
        except Exception as exists_exception:
            Log.exception(ClientError.ERROR_PATH_EXISTS_UNHANDLED_EXCEPTION, exists_exception)

        return path in file_list_result

    def file_exists(self, bucket, filename) -> bool:
        """
        Check if file exists in the specified bucket

        :type bucket:str
        :param bucket: Bucket to be searched

        :type filename:str
        :param filename: Path/filename to search for in bucket

        :return: bool
        """
        # Sanitize the filename
        filename = self.sanitize_filename(filename)

        file_list_result = []
        try:
            file_list_result = self.file_list(bucket=bucket, path=Client.sanitize_path(os.path.dirname(filename)))
        except Exception as exists_exception:
            Log.exception(ClientError.ERROR_FILE_EXISTS_UNHANDLED_EXCEPTION, exists_exception)

        return filename in file_list_result

    def path_delete(self, bucket, path, allow_missing=False) -> None:
        """
        Delete a path from S3 bucket

        :type bucket: str
        :param bucket: Bucket from which the file should be deleted

        :type path: str
        :param path: Path to be deleted

        :type allow_missing: bool
        :param allow_missing: Boolean flag, if False and the file cannot be found to delete an exception will be raised

        :return: None
        """
        # Sanitize the path
        path = self.sanitize_path(path)

        # Make sure the file exists before we try to delete it
        if self.path_exists(bucket=bucket, path=path) is False:
            # If this is fine, get out of here- the file is already gone
            if allow_missing is True:
                return

            Log.exception(ClientError.ERROR_PATH_DELETE_NOT_FOUND)

        # Delete the path
        try:
            self.__get_boto3_s3_client__().delete_object(Bucket=bucket, Key=path)
        except Exception as delete_exception:
            Log.exception(ClientError.ERROR_PATH_DELETE_UNHANDLED_EXCEPTION, delete_exception)

        # Make sure the file no longer exists after deleting
        if self.path_exists(bucket=bucket, path=path) is True:
            Log.exception(ClientError.ERROR_PATH_DELETE_FAILED)

    def file_delete(self, bucket, filename, allow_missing=False) -> None:
        """
        Delete a file from S3 bucket

        :type bucket:str
        :param bucket: Bucket from which the file should be deleted

        :type filename:str
        :param filename: Path of the S3 file to be deleted

        :type allow_missing: bool
        :param allow_missing: Boolean flag, if False and the file cannot be found to delete an exception will be raised

        :return: None
        """
        # Sanitize the filename
        filename = self.sanitize_filename(filename)

        # Make sure the file exists before we try to delete it
        if self.file_exists(bucket=bucket, filename=filename) is False:
            # If this is fine, get out of here- the file is already gone
            if allow_missing is True:
                return
            Log.exception(ClientError.ERROR_FILE_DELETE_NOT_FOUND)

        # Delete the file
        try:
            self.__get_boto3_s3_client__().delete_object(Bucket=bucket, Key=filename)
        except Exception as delete_exception:
            Log.exception(ClientError.ERROR_FILE_DELETE_UNHANDLED_EXCEPTION, delete_exception)

        # Make sure the file no longer exists after deleting
        if self.file_exists(bucket=bucket, filename=filename) is True:
            Log.exception(ClientError.ERROR_FILE_DELETE_FAILED)

    def file_move(self, source_bucket, source_filename, destination_bucket, destination_filename, allow_overwrite=True) -> None:
        """
        Move a file to the specified destination

        :type source_bucket:str
        :param source_bucket: The bucket the file should be moved from

        :type source_filename:str
        :param source_filename: The source filename

        :type destination_bucket:str
        :param destination_bucket: The bucket the file should be moved to

        :type destination_filename:str
        :param destination_filename: The destination filename

        :type allow_overwrite: bool
        :param allow_overwrite: Flag indicating the file is allowed to be overwritten if it already exists. If False, and the file exists an exception will be thrown

        :return: None
        """
        # Sanitize the filenames
        source_filename = Client.sanitize_filename(source_filename)
        destination_filename = Client.sanitize_filename(destination_filename)

        # Ensure the source and destination are not the same
        if source_filename == destination_filename:
            Log.exception(ClientError.ERROR_FILE_MOVE_SOURCE_DESTINATION_SAME)

        # If we are not in overwrite mode, we need to make sure the destination file doesn't already exist
        if allow_overwrite is False:
            if self.file_exists(bucket=destination_bucket, filename=destination_filename) is True:
                Log.exception(ClientError.ERROR_FILE_MOVE_ALREADY_EXISTS)

        # Make sure the source file exists
        if self.file_exists(bucket=source_bucket, filename=source_filename) is False:
            Log.exception(ClientError.ERROR_FILE_MOVE_SOURCE_NOT_FOUND)

        # Move the file
        try:
            self.file_copy(source_bucket=source_bucket, source_filename=source_filename, destination_bucket=destination_bucket, destination_filename=destination_filename)
        except Exception as move_exception:
            Log.exception(ClientError.ERROR_FILE_MOVE_COPY_UNHANDLED_EXCEPTION, move_exception)

        # Make sure the file exists at its destination
        if self.file_exists(bucket=destination_bucket, filename=destination_filename) is False:
            Log.exception(ClientError.ERROR_FILE_MOVE_COPY_FAILED)

        # Delete the file from the source bucket
        try:
            self.file_delete(bucket=source_bucket, filename=source_filename)
        except Exception as delete_exception:
            Log.exception(ClientError.ERROR_FILE_MOVE_DELETE_UNHANDLED_EXCEPTION, delete_exception)

        # Make sure the source file no longer exists
        if self.file_exists(bucket=source_bucket, filename=source_filename) is True:
            Log.exception(ClientError.ERROR_FILE_MOVE_DELETE_FAILED)

    def file_copy(self, source_bucket, source_filename, destination_bucket, destination_filename, allow_overwrite=True) -> None:
        """
        Copy file to the specified destination

        :type source_bucket:str
        :param source_bucket: The bucket the file should be copied from

        :type source_filename:str
        :param source_filename: The source path/filename

        :type destination_bucket:str
        :param destination_bucket: The bucket the file should be copied to

        :type destination_filename:str
        :param destination_filename: The destination path.filename

        :type allow_overwrite: bool
        :param allow_overwrite: Flag indicating the file is allowed to be overwritten if it already exists. If False, and the file exists an exception will be thrown

        :return: None
        """
        # Sanitize the filenames
        source_filename = Client.sanitize_filename(source_filename)
        destination_filename = Client.sanitize_filename(destination_filename)

        # Ensure the source and destination are not the same
        if source_filename == destination_filename:
            Log.exception(ClientError.ERROR_FILE_COPY_SOURCE_DESTINATION_SAME)

        # If we are not in overwrite mode, we need to check if the file exists already
        if allow_overwrite is False:
            if self.file_exists(bucket=destination_bucket, filename=destination_filename) is True:
                Log.exception(ClientError.ERROR_FILE_COPY_ALREADY_EXISTS)

        # Make sure the source file exists
        if self.file_exists(bucket=source_bucket, filename=source_filename) is False:
            Log.exception(ClientError.ERROR_FILE_COPY_SOURCE_NOT_FOUND)

        try:
            self.__get_boto3_s3_client__().copy(CopySource={'Bucket': source_bucket, 'Key': source_filename}, Bucket=destination_bucket, Key=destination_filename)
        except Exception as copy_exception:
            Log.exception(ClientError.ERROR_FILE_COPY_UNHANDLED_EXCEPTION, copy_exception)

        # Make sure the file exists at the destination
        if self.file_exists(bucket=destination_bucket, filename=destination_filename) is False:
            Log.exception(ClientError.ERROR_FILE_COPY_FAILED)

    def file_download(self, bucket, remote_filename, local_filename, allow_overwrite=True) -> None:
        """
        Download a file

        :type bucket:str
        :param bucket: Bucket from which the file should be downloaded

        :type remote_filename:str
        :param remote_filename: Path of the file to be downloaded in S3 bucket

        :type local_filename: str
        :param local_filename: Download filename on local sftp_filesystem

        :type allow_overwrite: bool
        :param allow_overwrite: Flag indicating the file is allowed to be overwritten if it already exists. If False, and the file exists an exception will be thrown

        :return: None
        """
        # Sanitize the filenames
        remote_filename = self.sanitize_filename(remote_filename)

        # If we are not in overwrite mode, we need to check if the file exists already
        if allow_overwrite is False:
            if os.path.exists(local_filename) is True:
                Log.exception(ClientError.ERROR_FILE_DOWNLOAD_ALREADY_EXISTS)

        # Make sure the file exists at the source
        if self.file_exists(bucket=bucket, filename=remote_filename) is False:
            Log.exception(ClientError.ERROR_FILE_DOWNLOAD_SOURCE_NOT_FOUND)

        # Download the file
        try:
            # Make sure the local download path exists
            destination_path = LocalDiskClient.sanitize_path(os.path.dirname(local_filename))
            LocalDiskClient.create_path(destination_path, allow_overwrite=True)

            self.__get_boto3_s3_client__().download_file(
                Bucket=bucket,
                Key=remote_filename,
                Filename=local_filename
            )
        except Exception as download_exception:
            Log.exception(ClientError.ERROR_FILE_DOWNLOAD_UNHANDLED_EXCEPTION, download_exception)

        # Make sure the file now exists locally
        if os.path.exists(local_filename) is False:
            Log.exception(ClientError.ERROR_FILE_DOWNLOAD_FAILED)

    def file_download_recursive(self, bucket, remote_path, local_path, callback=None, allow_overwrite=True) -> None:
        """
        Recursively download all files found in the specified remote path to the specified local path

        :type bucket: str
        :param bucket: The bucket to download files from

        :type remote_path: str
        :param remote_path: Path on SFTP server to be downloaded

        :type local_path: str
        :param local_path: Path on local file system where contents are to be downloaded

        :type callback: function/None
        :param callback: Optional callback_staked function to call after each file has downloaded successfully

        :type allow_overwrite: bool
        :param allow_overwrite: Flag indicating the file is allowed to be overwritten if it already exists. If False, and the file exists an exception will be thrown

        :return: None
        """
        # Sanitize the paths
        local_path = LocalDiskClient.sanitize_path(local_path)
        remote_path = self.sanitize_path(remote_path)

        # List files in current path
        files_found = self.file_list(bucket=bucket, path=remote_path, recursive=True)

        # Iterate these files
        for current_remote_filename in files_found:
            # The local filename will stored in the same folder structure as on the SFTP server
            current_local_path = LocalDiskClient.sanitize_path(local_path + os.path.dirname(current_remote_filename))
            current_local_filename = LocalDiskClient.sanitize_filename(current_local_path + os.path.basename(current_remote_filename))

            # Make sure the current files path exists locally before we start downloading
            LocalDiskClient.create_path(path=current_local_path, allow_overwrite=True)

            # Download the current file
            self.file_download(
                bucket=bucket,
                remote_filename=current_remote_filename,
                local_filename=current_local_filename,
                allow_overwrite=allow_overwrite
            )

            # If a callback_staked was supplied execute it
            if callback is not None:
                if callable(callback) is False:
                    Log.exception(ClientError.ERROR_FILE_DOWNLOAD_CALLBACK_NOT_CALLABLE)
                # If the callback_staked returns false, stop iterating
                if callback(local_filename=current_local_filename, remote_filename=current_remote_filename) is False:
                    break

    def file_upload(self, bucket, remote_filename, local_filename, allow_overwrite=True) -> None:
        """
        Upload a local file to the specified location

        :type bucket:str
        :param bucket: Bucket where file should be uploaded

        :type remote_filename:str
        :param remote_filename: Destination filename in S3 bucket

        :type local_filename: str
        :param local_filename: File on local sftp_filesystem to be uploaded

        :type allow_overwrite: bool
        :param allow_overwrite: Flag indicating the file is allowed to be overwritten if it already exists. If False, and the file exists an exception will be thrown

        :return: None
        """
        # Sanitize the bucket path
        local_filename = LocalDiskClient.sanitize_filename(local_filename)
        remote_filename = self.sanitize_filename(remote_filename)

        # Make sure the local file exists
        if LocalDiskClient.file_exists(local_filename) is False:
            Log.exception(ClientError.ERROR_FILE_UPLOAD_SOURCE_NOT_FOUND)

        # Make sure the file doesn't already exist if overwrite is disabled
        if allow_overwrite is False:
            if self.file_exists(bucket=bucket, filename=remote_filename) is True:
                raise Exception(ClientError.ERROR_FILE_UPLOAD_ALREADY_EXISTS)

        # Upload the file
        try:
            self.__get_boto3_s3_client__().upload_file(Bucket=bucket, Key=remote_filename, Filename=local_filename)
        except Exception as upload_exception:
            Log.exception(ClientError.ERROR_FILE_UPLOAD_UNHANDLED_EXCEPTION, upload_exception)

        # Make sure the uploaded file exists
        if self.file_exists(bucket=bucket, filename=remote_filename) is False:
            Log.exception(ClientError.ERROR_FILE_UPLOAD_FAILED, remote_filename)

    def file_get_tags(self, bucket, filename) -> dict:
        """
        Return a list of tags on the specified file

        :type bucket:str
        :param bucket: Bucket in which the file is contained

        :type filename:str
        :param filename: Path of the S3 file

        :return: Dictionary of key/value pairs representing the files tags
        """
        # Sanitize the bucket path
        filename = Client.sanitize_filename(filename)

        Log.trace('Retrieving tags for S3 file...')
        Log.debug('Bucket Name: {bucket_name}'.format(bucket_name=bucket))
        Log.debug('Bucket Filename: {bucket_filename}'.format(bucket_filename=filename))

        # Make sure the file exists before we try to do this
        Log.debug('Checking File Exists Before Reading Tags...')
        if self.file_exists(bucket=bucket, filename=filename) is False:
            # The file to be deleted did not exist
            Log.exception(ClientError.ERROR_FILE_GET_TAGS_SOURCE_NOT_FOUND)

        # Retrieve the existing tags
        keys = None
        object_tags = None
        try:
            Log.debug('Loading Tag Set...')
            object_tags = self.__get_boto3_s3_client__().get_object_tagging(Bucket=bucket, Key=filename)
            keys = object_tags.keys()
        except Exception as tag_exception:
            Log.exception(ClientError.ERROR_FILE_GET_TAGS_UNHANDLED_EXCEPTION, tag_exception)

        # Check if any tags existed
        if 'TagSet' not in keys:
            # No tags found, return the empty dictionary
            Log.debug('No Tags Found')
            return {}

        # Iterate tags into a dictionary
        Log.debug('Tags Found')
        tags = {}
        for tag in object_tags['TagSet']:
            key = tag['Key']
            value = tag['Value']
            Log.debug('- {key}: {value}'.format(key=key, value=value))
            tags[key] = value

        # Return the tags we found
        return tags

    def file_set_tags(self, bucket, filename, tags) -> None:
        """
        Replace all tags on a file with those specified

        :type bucket: str
        :param bucket: Name of the bucket

        :type filename: str
        :param filename: Name of the file

        :type tags: dict
        :param tags: Dictionary of key/value pairs that represent that tags to set

        :return: None
        """
        # Sanitize the bucket path
        filename = self.sanitize_filename(filename)

        Log.trace('Setting AWS S3 file tags...')
        Log.debug('Bucket Name: {bucket_name}'.format(bucket_name=bucket))
        Log.debug('Bucket Filename: {bucket_filename}'.format(bucket_filename=filename))

        # Make sure the file exists before we try to do this
        Log.debug('Checking File Exists Before Setting Tags...')
        if self.file_exists(bucket=bucket, filename=filename) is False:
            # The file to be deleted did not exist
            Log.exception(ClientError.ERROR_FILE_GET_TAGS_SOURCE_NOT_FOUND)

        # Create list object to pass to S3 with key/value pairs
        tag_set = []
        for key in tags.keys():
            Log.debug('Tag: {key}={value}'.format(key=key, value=tags[key]))
            tag_set.append({'Key': key, 'Value': tags[key]})

        try:
            Log.debug('Writing tags to S3 object...')
            self.__get_boto3_s3_client__().put_object_tagging(
                Bucket=bucket,
                Key=filename,
                Tagging={'TagSet': tag_set}
            )
        except Exception as put_tag_exception:
            Log.exception(ClientError.ERROR_FILE_PUT_TAGS_UNHANDLED_EXCEPTION, put_tag_exception)

        # Retrieve the tags were written correctly
        Log.debug('Checking Tags...')
        new_tags = self.file_get_tags(bucket=bucket, filename=filename)

        # Validate the number of keys match
        if len(new_tags) != len(tags):
            Log.exception(ClientError.ERROR_FILE_PUT_TAGS_FAILED)

        # Validate each tag is present in the new values and the values match
        for key in tags.keys():
            if key not in new_tags:
                # The tag did not exist
                Log.exception(ClientError.ERROR_FILE_PUT_TAGS_FAILED)
            if tags[key] != new_tags[key]:
                # The values were not matched
                Log.exception(ClientError.ERROR_FILE_PUT_TAGS_FAILED)

    # Internal methods

    def __get_boto3_s3_client__(self):
        """
        Retrieve Boto3 S3 client

        :return:
        """
        if self.__boto3_s3_client__ is None:
            self.__boto3_s3_client__ = boto3.session.Session().client('s3')

        return self.__boto3_s3_client__
