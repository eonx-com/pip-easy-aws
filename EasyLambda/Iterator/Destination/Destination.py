from EasyLambda.EasyFilesystem import Filesystem


class Destination:
    def __init__(
            self,
            filesystem,
            create_timestamped_folder,
            create_logfile_on_completion,
            allow_overwrite
    ):
        """
        Setup source filesystem

        :type filesystem: Filesystem
        :param filesystem: The underlying filesystem this destination is using

        :type create_timestamped_folder: bool
        :param create_timestamped_folder: If True, files will be placed into a ISO-8601 timestamped folder (the time at which the iteration process began) underneath the destination filesystems base path

        :type create_logfile_on_completion: bool
        :param create_logfile_on_completion: If True, the current Lambda function log history of the current execution will be saved to the destination folder if an error occurs
        """
        self.__filesystem__ = filesystem
        self.__create_timestamped_folder__ = create_timestamped_folder
        self.__create_logfile_on_error__ = create_logfile_on_completion
        self.__allow_overwrite__ = allow_overwrite

    def get_filesystem(self) -> Filesystem:
        """
        Return the underlying filesystem

        :return: Filesystem
        """
        return self.__filesystem__

    def is_create_timestamped_folder_enabled(self) -> bool:
        """
        Return boolean flag indicating whether files should be saved to a timestamped folder inside
        the base path

        :return: bool
        """
        return self.__create_timestamped_folder__

    def is_create_logfile_on_completion_enabled(self) -> bool:
        """
        Return boolean flag indicating whether a logfile should be created in this destination on
        completion of all iteration

        :return: bool
        """
        return self.__create_logfile_on_error__

    def is_allow_overwrite_enabled(self) -> bool:
        """
        Return boolean flag indicating whether files in the bucket are allowed to be overwritten.

        :return: bool
        """
        return self.__allow_overwrite__



    def file_upload(self, local_filename, destination_filename):
        """
        Upload file from local filesystem to destination

        :type local_filename: str
        :param local_filename: Path/filename of local file to be uploaded

        :type destination_filename: str
        :param destination_filename: Path/filename in destination folder

        :return: None
        """
        self.__filesystem__.upload(
            local_filename=local_filename,
            remote_filename=destination_filename
        )
