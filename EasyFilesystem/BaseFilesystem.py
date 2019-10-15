import uuid
from abc import abstractmethod
from typing import Optional

from EasyFilesystem.BaseFilesystemError import BaseFilesystemError
from EasyLocalDisk.Client import Client as LocalDiskClient
from EasyLog.Log import Log


class BaseFilesystem:
    ITERATE_STRATEGY_IGNORE = 'ignore'
    ITERATE_STRATEGY_RENAME = 'rename'

    def __init__(self):
        """
        Initialize common sftp_filesystem variables
        """
        self.__uuid__ = uuid.uuid4()
        self.__file_download_limit__ = None
        self.__file_download_count__ = 0
        self.__temp_paths__ = []
        self.__callback_staked__ = None
        self.__callback_success__ = None
        self.__callback_error__ = None

    @abstractmethod
    def create_path(self, path, allow_overwrite=False) -> None:
        """
        Create path in remote sftp_filesystem

        :type path: str
        :param path: The path to create

        :type allow_overwrite: bool
        :param allow_overwrite: Flag indicating the path is allowed to be overwritten if it already exists. If False, and the path exists an exception will be thrown
        """
        pass

    @abstractmethod
    def create_temp_path(self, prefix='', temp_path=None) -> str:
        """
        Create a new temporary path that is guaranteed to be unique

        :type prefix: str
        :param prefix: Optional prefix to prepend to path

        :type temp_path: str or None
        :param temp_path: The base path for all temporary files. If None a sensible default should be set

        :return: The path to that was created
        """
        pass

    @abstractmethod
    def file_list(self, path, recursive=False) -> list:
        """
        List the contents of the specified bucket/path

        :type path:str
        :param path: The buckets path

        :type recursive: bool
        :param recursive: If true all sub-folder of the path will be iterated

        :return: List of filenames
        """
        pass

    @abstractmethod
    def file_exists(self, filename) -> bool:
        """
        Check if file exists

        :type filename:str
        :param filename: Path/filename to search for

        :return: Boolean flag indicating whether the file exists
        """
        pass

    @abstractmethod
    def path_exists(self, path) -> bool:
        """
        Check if path exists

        :type path:str
        :param path: Path to check

        :return: Boolean flag indicating whether the path exists
        """
        pass

    @abstractmethod
    def file_delete(self, filename, allow_missing=False) -> None:
        """
        Delete a file

        :type filename:str
        :param filename: Path of the file to be deleted

        :type allow_missing: bool
        :param allow_missing: Boolean flag, if False and the file cannot be found to delete an exception will be raised
        """
        pass

    @abstractmethod
    def path_delete(self, path, allow_missing=False) -> None:
        """
        Delete a path

        :type path:str
        :param path: to be deleted

        :type allow_missing: bool
        :param allow_missing: Boolean flag, if False and the file cannot be found to delete an exception will be raised
        """
        pass

    @abstractmethod
    def file_move(self, source_filename, destination_filename, allow_overwrite=True) -> None:
        """
        Move a file to the specified destination

        :type source_filename:str
        :param source_filename: The source filename

        :type destination_filename:str
        :param destination_filename: The destination filename

        :type allow_overwrite: bool
        :param allow_overwrite: Flag indicating the file is allowed to be overwritten if it already exists. If False, and the file exists an exception will be thrown
        """
        pass

    @abstractmethod
    def file_copy(self, source_filename, destination_filename, allow_overwrite=True) -> None:
        """
        Copy file to the specified destination

        :type source_filename:str
        :param source_filename: The source path/filename

        :type destination_filename:str
        :param destination_filename: The destination path.filename

        :type allow_overwrite: bool
        :param allow_overwrite: Flag indicating the file is allowed to be overwritten if it already exists. If False, and the file exists an exception will be thrown
        """
        pass

    @abstractmethod
    def file_download(self, local_filename, remote_filename, allow_overwrite=True) -> None:
        """
        Download a file

        :type local_filename: str
        :param local_filename: Filename/path to destination on local sftp_filesystem where the file will be downloaded to

        :type remote_filename: str
        :param remote_filename: Filename/path of the remote file to be downloaded

        :type allow_overwrite: bool
        :param allow_overwrite: Flag indicating the file is allowed to be overwritten if it already exists. If False, and the file exists an exception will be raised

        :return: None
        """
        pass

    @abstractmethod
    def file_download_recursive(self, remote_path, local_path, callback=None, allow_overwrite=True) -> None:
        """
        Recursively download all files found in the specified remote path to the specified local path

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
        pass

    @abstractmethod
    def file_upload(self, local_filename, remote_filename, allow_overwrite=True) -> None:
        """
        Upload a file to remote sftp_filesystem

        :type local_filename: str
        :param local_filename: Filename/path of file to be uploaded from local sftp_filesystem

        :type remote_filename: str
        :param remote_filename: Filename/path where the file should be uploaded

        :type allow_overwrite: bool
        :param allow_overwrite: Flag indicating the file is allowed to be overwritten if it already exists. If False, and the file exists an exception will be raised

        :return: None
        """
        pass

    # Download limit tracking

    def reset_file_download_limit(self) -> None:
        """
        Reset the file download counter used to track maximum number of allowed downloads

        :return: None
        """
        self.__file_download_count__ = 0

    def increment_download_counter(self) -> bool:
        """
        Increase the download counter and return a flag indicating whether the limit has been reached

        :return: bool
        """
        if self.__file_download_limit__ is not None:
            self.__file_download_count__ += 1

        return self.is_file_download_limit_reached()

    def is_file_download_limit_reached(self) -> bool:
        """
        Return boolean flag indicating whether the download limit has been reached

        :return: bool
        """
        if self.__file_download_limit__ is None:
            return False

        return self.__file_download_limit__ - self.__file_download_count__ > 1

    def get_file_download_limit_remaining(self) -> Optional[int]:
        """
        Return the number of file downloads remaining

        :return: int or None
        """
        # If there is download limit set, return None
        if self.__file_download_limit__ is None:
            return None

        # Otherwise return the number of files remaining
        return self.__file_download_limit__ - self.__file_download_count__

    def set_file_download_limit(self, maximum_files) -> None:
        """
        Set a limit on the maximum number of downloads allowed

        :type maximum_files int
        :param maximum_files: The maximum number of files that can be downloaded. Once reached any download requested will be skipped

        :return: None
        """
        self.__file_download_limit__ = maximum_files

    # Iteration

    def iterate_files(
            self,
            staking_strategy,
            callback_staked,
            callback_success=None,
            callback_error=None,
            maximum_files=None
    ) -> None:
        """
        Iterate sftp_filesystem triggering user callback on each file

        :type maximum_files: int or None
        :param maximum_files: Maximum number files to stake

        :type staking_strategy: str
        :param staking_strategy: Staking strategy to use

        :type callback_staked: Callable
        :param callback_staked: User callback function executed for each successfully staked file

        :type callback_success: Callable
        :param callback_success: User callback function executed after a file has been successfully staked/processed

        :type callback_error: Callable
        :param callback_error: User callback function executed if an error is encountered during staking/processing

        :return: None
        """
        Log.test('Creating Local Staking Path...')
        staking_path = LocalDiskClient.create_temp_path()

        # Test/store callback we will be using
        Log.test('Testing Processing Callback...')
        if callable(callback_staked) is False:
            Log.exception(BaseFilesystemError.ERROR_ITERATE_CALLBACK_NOT_CALLABLE)
        self.__callback_staked__ = callback_staked

        Log.test('Testing Success Callback...')
        if callback_success is not None:
            if callable(callback_success) is False:
                Log.exception(BaseFilesystemError.ERROR_ITERATE_CALLBACK_NOT_CALLABLE)
            self.__callback_success__ = callback_success

        Log.test('Testing Error Callback...')
        if callback_error is not None:
            if callable(callback_error) is False:
                Log.exception(BaseFilesystemError.ERROR_ITERATE_CALLBACK_NOT_CALLABLE)
            self.__callback_error__ = callback_error

        # Set download limit
        Log.test('Setting Download Limit...')
        self.set_file_download_limit(maximum_files=maximum_files)

        if staking_strategy == BaseFilesystem.ITERATE_STRATEGY_IGNORE:
            Log.test('Using Ignore Strategy')
            callback_staked = self.__stake_ignore__
        elif staking_strategy == BaseFilesystem.ITERATE_STRATEGY_RENAME:
            Log.test('Using Rename Strategy')
            callback_staked = self.__stake_rename__
        else:
            Log.exception(BaseFilesystemError.ERROR_ITERATE_STRATEGY_UNKNOWN)

        Log.test('Starting Recursive Download...')
        self.file_download_recursive(
            remote_path='',
            local_path=staking_path,
            callback=callback_staked
        )

    def __stake_ignore__(self, local_filename, remote_filename):
        """
        Stake file, ignoring any requirement for unique processing and just triggering the user
        callback_staked function (if any)

        :type local_filename: str
        :param local_filename:

        :type remote_filename: str
        :param remote_filename:
        """

    def __stake_rename__(self, local_filename, remote_filename):
        """
        Handle staked files ensuring we are the only process actioning the file by performing
        a rename operation with a unique value

        :type local_filename: str
        :param local_filename:

        :type remote_filename: str
        :param remote_filename:

        :return:
        """
        Log.test('Rename Callback Triggered...')

        staking_extension = 'easy-sftp_filesystem.staked'

        # If the file is always staked- ignore it
        if remote_filename.endswith(staking_extension) is True:
            Log.test('File Already Staked: {remote_filename}'.format(remote_filename=remote_filename))
            return

        staked_filename = '{remote_filename}.{uuid}.{staking_extension}'.format(
            remote_filename=remote_filename,
            uuid=uuid.uuid4(),
            staking_extension=staking_extension
        )

        Log.test('Staking Filename: {staked_filename}'.format(staked_filename=staked_filename))

        try:
            Log.test('Staking File...')
            self.file_move(
                source_filename=remote_filename,
                destination_filename=staked_filename,
                allow_overwrite=False
            )

            Log.test('Checking Staking Result...')
            if self.file_exists(staked_filename) is True:
                Log.test('Staking Success, Processing Callbacks...')
                self.__stake_process_callbacks__(
                    local_filename=local_filename,
                    remote_filename=remote_filename,
                    staked_filename=staked_filename
                )
        except Exception as staking_exception:
            Log.test('Failed To Stake File: {staking_exception}'.format(staking_exception=staking_exception))
            return

    def __stake_process_callbacks__(self, local_filename, remote_filename, staked_filename):
        # noinspection PyBroadException
        try:
            # Trigger staking callback
            if self.__callback_staked__ is not None:
                Log.test('Calling Staked Callback...')
                self.__callback_staked__(
                    filesystem=self,
                    local_filename=local_filename,
                    remote_filename=remote_filename,
                    staked_filename=staked_filename
                )
            # Trigger success callback (if any)
            if self.__callback_success__ is not None:
                Log.test('Calling Success Callback...')
                self.__callback_success__(
                    filesystem=self,
                    local_filename=local_filename,
                    remote_filename=remote_filename,
                    staked_filename=staked_filename
                )
        except Exception as staking_exception:
            Log.debug(staking_exception)
            # Trigger error callback (if any)
            if self.__callback_error__ is not None:
                Log.test('Calling Error Callback...')
                self.__callback_error__(
                    filesystem=self,
                    local_filename=local_filename,
                    remote_filename=remote_filename,
                    staked_filename=staked_filename,
                    staking_exception=staking_exception
                )
