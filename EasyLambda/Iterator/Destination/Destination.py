from EasyLambda.Iterator import Filesystem


class Destination:
    def __init__(
            self,
            filesystem,
            create_timestamped_folder,
            create_logfile_on_error
    ):
        """
        Setup source filesystem

        :type filesystem: Filesystem
        :param filesystem: The underlying filesystem this destination is using

        :type create_timestamped_folder: bool
        :param create_timestamped_folder: If True, files will be placed into a ISO-8601 timestamped folder (the time at which the iteration process began) underneath the destination filesystems base path

        :type create_logfile_on_error: bool
        :param create_logfile_on_error: If True, the current Lambda function log history of the current execution will be saved to the destination folder if an error occurs
        """
        self.__filesystem__ = filesystem
        self.__create_timestamped_folder__ = create_timestamped_folder
        self.__create_logfile_on_error__ = create_logfile_on_error
