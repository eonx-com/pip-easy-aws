import uuid
from abc import abstractmethod
from typing import Optional


class BaseFilesystem:
    def __init__(self):
        """
        Initialize common filesystem variables
        """
        self.__uuid__ = uuid.uuid4()
        self.__file_download_limit__ = None
        self.__file_download_count__ = 0

    @abstractmethod
    def create_path(self, path, allow_overwrite=False) -> None:
        """
        Create path in remote filesystem

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

        :type path: string
        :param path: The buckets path

        :type recursive: bool
        :param recursive: If true all sub-folder of the path will be iterated

        :return: List of filenames
        """
        pass

    @abstractmethod
    def file_exists(self, filename) -> bool:
        """
        Check if file exists in the specified bucket

        :type filename: string
        :param filename: Path/filename to search for in bucket

        :return: Boolean flag indicating whether the file exists
        """
        pass

    @abstractmethod
    def file_delete(self, filename, allow_missing=False) -> None:
        """
        Delete a file

        :type filename: string
        :param filename: Path of the file to be deleted

        :type allow_missing: bool
        :param allow_missing: Boolean flag, if False and the file cannot be found to delete an exception will be raised
        """
        pass

    @abstractmethod
    def file_move(self, source_filename, destination_filename, allow_overwrite=True) -> None:
        """
        Move a file to the specified destination

        :type source_filename: string
        :param source_filename: The source filename

        :type destination_filename: string
        :param destination_filename: The destination filename

        :type allow_overwrite: bool
        :param allow_overwrite: Flag indicating the file is allowed to be overwritten if it already exists. If False, and the file exists an exception will be thrown
        """
        pass

    @abstractmethod
    def file_copy(self, source_filename, destination_filename, allow_overwrite=True) -> None:
        """
        Copy file to the specified destination

        :type source_filename: string
        :param source_filename: The source path/filename

        :type destination_filename: string
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
        :param local_filename: Filename/path to destination on local filesystem where the file will be downloaded to

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
        :param callback: Optional callback function to call after each file has downloaded successfully

        :type allow_overwrite: bool
        :param allow_overwrite: Flag indicating the file is allowed to be overwritten if it already exists. If False, and the file exists an exception will be thrown

        :return: None
        """
        pass

    @abstractmethod
    def file_upload(self, local_filename, remote_filename, allow_overwrite=True) -> None:
        """
        Upload a file to remote filesystem

        :type local_filename: str
        :param local_filename: Filename/path of file to be uploaded from local filesystem

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

    def iterate_files(self, callback, strategy, maximum_files=None) -> None:
        """
        :type maximum_files: int or None
        :param maximum_files: Maximum number files to stake

        :type callback: Callable
        :param callback: User callback function executed on every successfully staked file

        :type strategy: str
        :param strategy: Staking strategy to use

        :return: None
        """
        staking_path = LocalDiskClient.create_temp_path()

        if callable(callback) is False:
            Log.exception(FilesystemError.ERROR_ITERATE_CALLBACK_NOT_CALLABLE)

        self.__sftp_client__.set_file_download_limit(maximum_files=maximum_files)

        if strategy == Filesystem.ITERATE_STRATEGY_IGNORE:
            callback = self.__stake_ignore__
        elif strategy == Filesystem.ITERATE_STRATEGY_RENAME:
            callback = self.__stake_rename__
        elif strategy == Filesystem.ITERATE_STRATEGY_TAG:
            callback = self.__stake_tag__
        else:
            Log.exception(FilesystemError.ERROR_ITERATE_STRATEGY_UNKNOWN)

        self.file_download_recursive(
            remote_path='',
            local_path=staking_path,
            callback=callback
        )

    def __stake_ignore__(self, local_filename, remote_filename):
        """
        Handle staked files as they are downloaded

        :type local_filename: str
        :param local_filename:

        :type remote_filename: str
        :param remote_filename:

        :return:
        """
        pass

    def __stake_move__(self, local_filename, remote_filename):
        """
        Handle staked files as they are downloaded, ensuring we are the only process actioning the file by performing a rename operation

        :type local_filename: str
        :param local_filename:

        :type remote_filename: str
        :param remote_filename:

        :return:
        """
        pass
