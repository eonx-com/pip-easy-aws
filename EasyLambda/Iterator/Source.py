from EasyLambda.Iterator.Filesystem import Filesystem


class Source:
    def __init__(
            self,
            filesystem,
            delete_on_success,
            delete_on_error
    ):
        """
        This should not be called directly, use the SourceFactory methods to create a source filesystem

        :type filesystem: Filesystem
        :param filesystem: The underlying filesystem this destination is using

        :type delete_on_success: bool
        :param delete_on_success: If True, files will be deleted from the source on successful iteration

        :type delete_on_error: bool
        :param delete_on_error: If True, files will be deleted from the source if an error occurs during iteration
        """
        self.__filesystem__ = filesystem
        self.__delete_on_success__ = delete_on_success
        self.__delete_on_error__ = delete_on_error

    def iterate_files(
            self,
            callback,
            filesystems,
            maximum_files=None,
    ):
        """
        Iterate files from the current source

        :type callback: function
        :param callback: User callback function that is executed for each iterated file

        :type filesystems: dict
        :param filesystems: The iterators filesystems

        :type maximum_files: int or None
        :param maximum_files: The maximum number of files to iterate

        :return: list List of files that were iterated over
        """
        return self.__filesystem__.iterate_files(
            callback=callback,
            filesystems=filesystems,
            maximum_files=maximum_files
        )