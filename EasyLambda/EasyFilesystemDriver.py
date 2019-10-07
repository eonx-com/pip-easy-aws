from abc import abstractmethod


class EasyFilesystemDriver:
    # Error constants
    ERROR_ITERATION_FAILURE = 'An unexpected error occurred during iteration of the current file'

    @abstractmethod
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
        pass

    @abstractmethod
    def file_list(self, filesystem_path, recursive=False) -> list:
        """
        List files in the filesystem path

        :type filesystem_path: str
        :param filesystem_path: Path in the filesystem to list (relative to whatever base path may be defined)

        :type recursive: bool
        :param recursive: Flag indicating the listing should be recursive. If False, sub-folder contents will not be returned

        :return: list
        """
        pass

    @abstractmethod
    def file_exists(self, filesystem_filename) -> bool:
        """
        Check if file exists in the filesystem

        :type filesystem_filename: str
        :param filesystem_filename: The name of the file to check in the filesystem (relative to whatever base path may be defined)

        :return: bool
        """
        pass

    @abstractmethod
    def file_delete(self, filesystem_filename) -> None:
        """
        Delete a file from the filesystem

        :type filesystem_filename: str
        :param filesystem_filename: The name of the file to be deleted from the filesystem (relative to whatever base path may be defined)

        :return: None
        """
        pass

    @abstractmethod
    def file_download(self, filesystem_filename, local_filename):
        """
        Download a file from the filesystem to local storage

        :type filesystem_filename: str
        :param filesystem_filename: The name of the file in the filesystem to download (relative to whatever base path may be defined)

        :type local_filename: str
        :param local_filename: The destination on the local filesystem where the file will be downloaded to

        :return:
        """
        pass

    @abstractmethod
    def file_upload(self, filesystem_filename, local_filename):
        """
        Upload a file from local storage to the filesystem

        :type filesystem_filename: str
        :param filesystem_filename: The destination on the filesystem where the file will be uploaded to  (relative to whatever base path may be defined)

        :type local_filename: str
        :param local_filename: The name of the local filesystem file that will be uploaded

        :return: None
        """
        pass
